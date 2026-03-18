"""
Planner — Gemini Call 1 (internal, fast).

Takes lesson context and decides the presentation STRUCTURE:
how many slides, in what order, which template type for each.

This output (a slide plan) is never shown to the teacher — it's an
internal step that drives content_generator.py.
"""

import asyncio
import json
import logging
import random
import re
from google import genai
from google.genai import types
from config import Config
from templates import ALL_TEMPLATE_TYPES

logger = logging.getLogger(__name__)

def _make_client() -> genai.Client:
    """Create a Gemini client, routing through Helicone proxy when configured."""
    if Config.HELICONE_API_KEY:
        loc = Config.VERTEX_AI_LOCATION
        target_host = (
            "aiplatform.googleapis.com"
            if loc == "global"
            else f"{loc}-aiplatform.googleapis.com"
        )
        return genai.Client(
            vertexai=True,
            project=Config.GOOGLE_CLOUD_PROJECT,
            location=loc,
            http_options=types.HttpOptions(
                base_url="https://gateway.helicone.ai",
                headers={
                    "Helicone-Auth": f"Bearer {Config.HELICONE_API_KEY}",
                    "Helicone-Target-Url": f"https://{target_host}",
                },
            ),
        )
    return genai.Client(
        vertexai=True,
        project=Config.GOOGLE_CLOUD_PROJECT,
        location=Config.VERTEX_AI_LOCATION,
    )


_client = _make_client()

# ── Template descriptions for the planning prompt ─────────────────────

_TEMPLATE_GUIDE = """
Các loại template có sẵn (templateType):

TEMPLATES CÓ SẴN (6 template FE, tạo ra templateId):
- template-001: Image bên trái (1/3) + Text bên phải (2/3). Dùng khi slide về chủ đề trực quan: bản đồ, biểu đồ, hình ảnh địa lý. Ảnh là trọng tâm.
- template-002: Text bên trái (2/3) + Image bên phải (1/3). Dùng khi giải thích trước, hình ảnh là minh họa hỗ trợ.
- template-003: Tiêu đề căn giữa + 2 cột text NGẮN (1-3 dòng mỗi cột). Dùng so sánh 2 thứ: 2 loại phép chiếu, 2 vùng, nguyên nhân/kết quả.
- template-004: Tiêu đề căn giữa + 2 cột text DÀY (3+ dòng mỗi cột). Dùng so sánh chi tiết, giải thích cặp khái niệm.
- template-005: Tiêu đề căn giữa + 3 cột text NGẮN. Dùng khi có ĐÚNG 3 loại/danh mục với mô tả ngắn.
- template-006: Tiêu đề căn giữa + 3 cột text DÀY. Dùng so sánh 3 chiều chi tiết (mỗi mục cần 1 đoạn).

CHÚ Ý: template-001 và template-002 sẽ tạo placeholder cho ảnh (giáo viên thêm ảnh sau). Dùng khi nội dung cần minh họa trực quan (bản đồ, biểu đồ, hình ảnh địa lý).

FREEFORM (không có templateId, dùng cho các slide đặc biệt):
- TITLE_CARD: Slide đầu tiên DUY NHẤT. Tên bài + thông tin môn học.
- BULLET_CARD: Danh sách 4+ mục không vừa với 2-3 cột. VD: mục tiêu học, ứng dụng thực tế.
- SECTION_DIV: Slide chuyển tiếp giữa các chủ đề lớn. Tiêu đề lớn, nền tối. KHÔNG dùng liên tiếp.
- QUIZ_CARD: Sau mỗi phần nội dung chính. 2-4 câu hỏi trắc nghiệm từ khái niệm đã học. KHÔNG dùng liên tiếp.
- FLASHCARD_CARD: Phần ôn tập. Mặt trước = tên khái niệm, mặt sau = định nghĩa.
- FILL_BLANK_CARD: Ôn tập tích cực. Câu key từ SGK với chỗ trống.
- SUMMARY_CARD: Slide CUỐI CÙNG. Tóm tắt 4-6 ý chính.
"""

_SYSTEM_PROMPT = (
    "Bạn là chuyên gia thiết kế bài thuyết trình giáo dục cho chương trình phổ thông Việt Nam. "
    "Nhiệm vụ của bạn là lập kế hoạch cấu trúc bài thuyết trình dựa trên dữ liệu đánh giá giáo án và nội dung sách giáo khoa. "
    "Chỉ trả về JSON, không có markdown fence, không có giải thích."
)


def _parse_json(raw: str) -> dict | list:
    """Strip markdown fences and parse JSON."""
    cleaned = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()
    return json.loads(cleaned)


async def plan_presentation(context: dict, slide_range: str) -> list[dict]:
    """
    Call Gemini to produce a slide plan.

    Args:
        context: dict with lesson data:
            - lesson_name (str)
            - subject (str)
            - grade (str)
            - objectives (list[str])
            - covered_yeu_cau (list[{noi_dung, how}])
            - missing_yeu_cau (list[{noi_dung, tieu_chuan, importance}])
            - chu_de_list (list[{id, ten_chu_de, phan_mon}])
            - activities (list[str])
            - textbook_sections (list[{heading, content}])  ← truncated summary
        slide_range: "short" | "medium" | "detailed"

    Returns:
        list of slide plan dicts:
        [
          {
            "index": 0,
            "templateType": "TITLE_CARD",
            "title": "Tên bài học",
            "focus": "Mô tả ngắn nội dung cần đưa vào slide này",
            "conceptRefs": []   ← tên khái niệm liên quan (để content_generator dùng)
          },
          ...
        ]
    """
    min_slides, max_slides = Config.SLIDE_RANGE_MAP.get(slide_range, (10, 15))
    interactive_count = Config.INTERACTIVE_SLIDE_MAP.get(slide_range, 2)

    # Build a concise textbook summary (cap at ~2000 chars to stay within context)
    textbook_summary = ""
    for sec in context.get("textbook_sections", []):
        textbook_summary += f"\n[{sec.get('heading', '')}]\n{sec.get('content', '')[:300]}\n"
        if len(textbook_summary) > 2000:
            textbook_summary = textbook_summary[:2000] + "\n...(rút gọn)\n"
            break

    covered_yeu_cau = context.get("covered_yeu_cau", [])
    missing_yeu_cau = context.get("missing_yeu_cau", [])
    chu_de_list = context.get("chu_de_list", [])
    objectives = context.get("objectives", [])
    activities = context.get("activities", [])

    # Pre-compute JSON strings — dict literals inside f-string expressions cause
    # "unhashable type: dict" because {{...}} is parsed as a set, not escaped braces.
    covered_json = json.dumps(
        [{"noi_dung": yc.get("noi_dung"), "how": yc.get("how")} for yc in covered_yeu_cau],
        ensure_ascii=False, indent=2,
    )
    missing_json = json.dumps(
        [{"noi_dung": yc.get("noi_dung"), "tieu_chuan": yc.get("tieu_chuan"), "importance": yc.get("importance")} for yc in missing_yeu_cau],
        ensure_ascii=False, indent=2,
    )

    user_prompt = f"""
Thông tin bài học:
- Tên bài: {context.get("lesson_name", "Không rõ")}
- Môn: {context.get("subject", "")} Lớp {context.get("grade", "")}

Mục tiêu bài học:
{json.dumps(objectives, ensure_ascii=False, indent=2)}

Chủ đề trong chương trình:
{json.dumps([cd.get("ten_chu_de") for cd in chu_de_list], ensure_ascii=False, indent=2)}

Yêu cầu cần đạt đã phủ trong giáo án:
{covered_json}

Yêu cầu cần đạt còn thiếu (quan trọng cần bổ sung):
{missing_json}

Hoạt động dạy học:
{json.dumps(activities, ensure_ascii=False, indent=2)}

Nội dung SGK (tóm tắt):
{textbook_summary}

{_TEMPLATE_GUIDE}

Quy tắc bắt buộc:
1. Slide đầu tiên PHẢI là TITLE_CARD
2. Slide cuối cùng PHẢI là SUMMARY_CARD
3. Số slide NỘI DUNG (không tính interactive): tối thiểu {min_slides}, tối đa {max_slides}
4. Dùng đa dạng template, không lặp cùng 1 template 3 lần liên tiếp
5. Đặt SECTION_DIV trước mỗi chủ đề lớn mới (nếu có từ 2 chủ đề trở lên)
6. NGOÀI các slide nội dung, thêm ĐÚNG {interactive_count} slide tương tác (QUIZ_CARD, FLASHCARD_CARD, hoặc FILL_BLANK_CARD — chọn loại phù hợp). Đặt chúng xen kẽ trong bài, KHÔNG gộp cuối cùng.
7. Dùng template-001 hoặc template-002 khi slide cần minh họa trực quan (ảnh sẽ là placeholder)
8. "focus" phải đủ chi tiết để AI có thể viết nội dung từ đó (1-2 câu mô tả rõ ràng)

Trả về JSON array theo định dạng sau (CHỈ JSON, không có markdown):
[
  {{
    "index": 0,
    "templateType": "TITLE_CARD",
    "title": "Tiêu đề slide bằng tiếng Việt",
    "focus": "Mô tả ngắn nội dung cần đưa vào slide này",
    "conceptRefs": ["tên khái niệm 1", "tên khái niệm 2"]
  }}
]
"""

    logger.info("Calling Gemini for slide planning (range: %s, %d-%d content + %d interactive)...",
                slide_range, min_slides, max_slides, interactive_count)

    _PLANNER_TIMEOUT = 120   # seconds before a hung planner call is cancelled
    _PLANNER_BASE_DELAY = 10  # base backoff seconds (doubles each attempt: 10s, 20s, 40s)
    _PLANNER_RETRY_CAP = 60   # max backoff ceiling in seconds

    last_error = None
    for attempt in range(4):  # up to 4 attempts
        try:
            response = await asyncio.wait_for(
                _client.aio.models.generate_content(
                    model=Config.GEMINI_MODEL,
                    contents=user_prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=_SYSTEM_PROMPT,
                        temperature=0.0,
                        response_mime_type="application/json",
                    ),
                ),
                timeout=_PLANNER_TIMEOUT,
            )
            last_error = None
            break
        except asyncio.TimeoutError as exc:
            last_error = exc
            if attempt < 3:
                ceiling = min(_PLANNER_BASE_DELAY * (2 ** attempt), _PLANNER_RETRY_CAP)
                wait = random.uniform(0, ceiling)
                logger.warning(
                    "Planner attempt %d timed out after %ds, retrying in %.1fs...",
                    attempt + 1, _PLANNER_TIMEOUT, wait,
                )
                await asyncio.sleep(wait)
            else:
                raise
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                ceiling = min(_PLANNER_BASE_DELAY * (2 ** attempt), _PLANNER_RETRY_CAP)
                wait = random.uniform(0, ceiling)
                logger.warning(
                    "Planner attempt %d failed (%s), retrying in %.1fs...",
                    attempt + 1, exc, wait,
                )
                await asyncio.sleep(wait)
            else:
                raise

    if last_error:
        raise last_error

    raw = response.text
    logger.debug("Planner raw response: %s", raw[:500])

    slide_plan = _parse_json(raw)

    if not isinstance(slide_plan, list):
        raise ValueError(f"Planner returned non-list JSON: {type(slide_plan)}")

    # Validate and sanitize
    validated = []
    for i, slide in enumerate(slide_plan):
        ttype = slide.get("templateType", "")
        if ttype not in ALL_TEMPLATE_TYPES:
            logger.warning("Slide %d has unknown templateType %r — defaulting to BULLET_CARD", i, ttype)
            slide["templateType"] = "BULLET_CARD"
        slide["index"] = i  # enforce sequential indexes
        slide.setdefault("conceptRefs", [])
        slide.setdefault("focus", "")
        validated.append(slide)

    logger.info("Planner produced %d slides.", len(validated))
    return validated
