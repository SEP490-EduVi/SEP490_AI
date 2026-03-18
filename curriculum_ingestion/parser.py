"""
Curriculum parser — two-step Gemini-based extraction.

Step 1: python-docx raw dump (dumb, zero classification logic).
         Converts the Word document into a flat text representation
         preserving paragraph styles and table structure.

Step 2: Single Gemini call with a strict JSON schema prompt.
         Gemini reads the raw dump and extracts the full curriculum
         structure regardless of formatting changes in future revisions.
"""

import json
import logging
import re
from dataclasses import dataclass, field

import docx
from google import genai
from google.genai import types

from config import Config

logger = logging.getLogger(__name__)


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class YeuCau:
    noi_dung: str       # Short topic label  e.g. "Thuyết kiến tạo mảng"
    tieu_chuan: str     # Full standard text e.g. "Trình bày được khái quát..."


@dataclass
class ChuDe:
    ten_chu_de: str             # e.g. "Trái Đất"
    phan_mon: str               # e.g. "Địa lí tự nhiên"
    yeu_cau_list: list[YeuCau] = field(default_factory=list)


@dataclass
class Lop:
    ten_lop: str                # e.g. "Lớp 10"
    chu_de_list: list[ChuDe] = field(default_factory=list)


@dataclass
class ParsedCurriculum:
    ten_mon: str                # e.g. "Địa lí"
    cap_hoc: str                # e.g. "THPT"
    lop_list: list[Lop] = field(default_factory=list)


# ── Step 1: python-docx raw dump ────────────────────────────────────────────

def _dump_document(docx_path: str) -> str:
    """
    Convert a .docx file into a plain-text representation.

    Paragraphs are emitted as:   [STYLE] text
    Table rows are emitted as:   | cell1 | cell2 | ...

    No classification is done here — just a faithful ordered dump.
    """
    doc = docx.Document(docx_path)
    lines: list[str] = []

    for element in doc.element.body:
        tag = element.tag.split("}")[-1]  # strip namespace

        if tag == "p":
            # Wrap as a docx Paragraph to access style + text
            para = docx.text.paragraph.Paragraph(element, doc)
            text = para.text.strip()
            if not text:
                continue
            style_name = para.style.name if para.style else "Normal"
            lines.append(f"[{style_name}] {text}")

        elif tag == "tbl":
            # Wrap as a docx Table
            table = docx.table.Table(element, doc)
            for row in table.rows:
                cells = [cell.text.strip().replace("\n", " ") for cell in row.cells]
                # Deduplicate adjacent identical cells (merged cell artefact in python-docx)
                deduped: list[str] = []
                for c in cells:
                    if not deduped or c != deduped[-1]:
                        deduped.append(c)
                lines.append("| " + " | ".join(deduped) + " |")

    return "\n".join(lines)


# ── Step 2: Gemini extraction ────────────────────────────────────────────────

_EXTRACTION_PROMPT = """\
Bạn là một chuyên gia phân tích văn bản giáo dục Việt Nam.

Dưới đây là nội dung thô (raw dump) của một file Word "Chương trình Giáo dục Phổ thông" do Bộ GD&ĐT ban hành.
Định dạng dump:
  - Đoạn văn: [Tên style] Nội dung văn bản
  - Hàng bảng: | Cột 1 | Cột 2 | ...

Nhiệm vụ của bạn: Trích xuất TOÀN BỘ cấu trúc "Nội dung cụ thể và Yêu cầu cần đạt" từ văn bản này.

=== QUY TẮC BẮT BUỘC ===

1. CHỈ trích xuất phần "NỘI DUNG GIÁO DỤC" (Mục V). Bỏ qua các phần I, II, III, IV, VI, VII, VIII.

2. ChuDe granularity (quan trọng):
   - Mỗi sub-topic riêng biệt là một ChuDe riêng.
   - Ví dụ: "Nông nghiệp, lâm nghiệp, thuỷ sản" / "Công nghiệp" / "Dịch vụ" là 3 ChuDe RIÊNG BIỆT.
   - KHÔNG gom chúng thành 1 ChuDe "Địa lí các ngành kinh tế".
   - Nhưng: "Địa lí các ngành kinh tế" trở thành giá trị `phan_mon` của 3 ChuDe đó.

3. Chuyên đề học tập (10.1, 10.2, 11.1, ...): Phải được ingest như ChuDe bình thường.
   - `phan_mon` = tên chuyên đề, ví dụ "Chuyên đề 10.1: Biến đổi khí hậu"
   - `ten_chu_de` = chủ đề chính của chuyên đề đó

4. YeuCau extraction:
   - Bảng 2 cột: cột trái = `noi_dung` (tên chủ đề/nội dung ngắn), cột phải = `tieu_chuan` (yêu cầu cần đạt đầy đủ).
   - Nếu một hàng có cột trái trống và cột phải có thêm yêu cầu → ghép vào `tieu_chuan` của YeuCau trước.
   - Giữ NGUYÊN VĂN nội dung, không tóm tắt, không dịch.

5. `ten_mon` và `cap_hoc`:
   - `ten_mon` = tên môn học (ví dụ: "Địa lí")
   - `cap_hoc` = "THPT" (trung học phổ thông)

=== OUTPUT FORMAT ===
Trả về CHỈ một JSON object, không có markdown fence, không có giải thích:

{{
  "ten_mon": "Địa lí",
  "cap_hoc": "THPT",
  "lop_list": [
    {{
      "ten_lop": "Lớp 10",
      "chu_de_list": [
        {{
          "ten_chu_de": "Trái Đất",
          "phan_mon": "Địa lí tự nhiên",
          "yeu_cau_list": [
            {{
              "noi_dung": "Sự hình thành Trái Đất, vỏ Trái Đất và vật liệu cấu tạo vỏ Trái Đất",
              "tieu_chuan": "Trình bày được nguồn gốc hình thành Trái Đất, đặc điểm của vỏ Trái Đất, các vật liệu cấu tạo vỏ Trái Đất."
            }}
          ]
        }}
      ]
    }}
  ]
}}

=== NỘI DUNG VĂN BẢN ===
{raw_dump}
"""


def _make_client() -> genai.Client:
    """Create a Gemini client, routing through Helicone proxy when configured."""
    if Config.HELICONE_API_KEY:
        return genai.Client(
            vertexai=True,
            project=Config.GOOGLE_CLOUD_PROJECT,
            location=Config.VERTEX_AI_LOCATION,
            http_options=types.HttpOptions(
                base_url="https://gateway.helicone.ai",
                headers={
                    "Helicone-Auth": f"Bearer {Config.HELICONE_API_KEY}",
                    "Helicone-Target-Url": f"https://{'aiplatform.googleapis.com' if Config.VERTEX_AI_LOCATION == 'global' else Config.VERTEX_AI_LOCATION + '-aiplatform.googleapis.com'}",
                },
            ),
        )
    return genai.Client(
        vertexai=True,
        project=Config.GOOGLE_CLOUD_PROJECT,
        location=Config.VERTEX_AI_LOCATION,
    )


def _strip_json_fence(text: str) -> str:
    """Remove markdown code fences if Gemini wraps the JSON anyway."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _validate_parsed(data: dict) -> None:
    """Raise ValueError if the JSON structure is missing required keys."""
    if "ten_mon" not in data or "lop_list" not in data:
        raise ValueError("Gemini response missing required keys: ten_mon, lop_list")
    for lop in data["lop_list"]:
        if "ten_lop" not in lop or "chu_de_list" not in lop:
            raise ValueError(f"Lop entry missing required keys: {lop}")
        for cd in lop["chu_de_list"]:
            if "ten_chu_de" not in cd or "yeu_cau_list" not in cd:
                raise ValueError(f"ChuDe entry missing required keys: {cd}")


def _to_dataclass(data: dict) -> ParsedCurriculum:
    """Convert the validated dict into typed dataclasses."""
    curriculum = ParsedCurriculum(
        ten_mon=data["ten_mon"],
        cap_hoc=data.get("cap_hoc", "THPT"),
    )
    for lop_data in data["lop_list"]:
        lop = Lop(ten_lop=lop_data["ten_lop"])
        for cd_data in lop_data["chu_de_list"]:
            chu_de = ChuDe(
                ten_chu_de=cd_data["ten_chu_de"],
                phan_mon=cd_data.get("phan_mon", ""),
            )
            for yc_data in cd_data["yeu_cau_list"]:
                chu_de.yeu_cau_list.append(YeuCau(
                    noi_dung=yc_data.get("noi_dung", ""),
                    tieu_chuan=yc_data.get("tieu_chuan", ""),
                ))
            lop.chu_de_list.append(chu_de)
        curriculum.lop_list.append(lop)
    return curriculum


# ── Public API ───────────────────────────────────────────────────────────────

def parse(docx_path: str) -> ParsedCurriculum:
    """
    Parse a curriculum Word file into a structured ParsedCurriculum.

    Step 1: Dump the docx to raw text (python-docx, zero classification).
    Step 2: Send to Gemini for structured extraction.
    Step 3: Validate + convert to dataclasses.
    """
    logger.info("Step 1: Dumping document to raw text: %s", docx_path)
    raw_dump = _dump_document(docx_path)
    logger.info("Raw dump: %d characters", len(raw_dump))

    logger.info("Step 2: Sending to Gemini for structured extraction...")
    client = _make_client()
    prompt = _EXTRACTION_PROMPT.format(raw_dump=raw_dump)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
        ),
    )

    raw_json = _strip_json_fence(response.text)

    logger.info("Step 3: Validating and converting Gemini response...")
    data = json.loads(raw_json)
    _validate_parsed(data)

    result = _to_dataclass(data)

    lop_count = len(result.lop_list)
    cd_count = sum(len(l.chu_de_list) for l in result.lop_list)
    yc_count = sum(len(cd.yeu_cau_list) for l in result.lop_list for cd in l.chu_de_list)
    logger.info(
        "Parsed: %d lớp, %d chủ đề, %d yêu cầu cần đạt",
        lop_count, cd_count, yc_count,
    )

    return result
