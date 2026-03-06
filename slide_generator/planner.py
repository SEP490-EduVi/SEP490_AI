"""
Planner — Gemini Call 1 (internal, fast).

Takes lesson context and decides the presentation STRUCTURE:
how many slides, in what order, which template type for each.

This output (a slide plan) is never shown to the teacher — it's an
internal step that drives content_generator.py.
"""

import json
import logging
import re
from google import genai
from google.genai import types
from config import Config
from templates import ALL_TEMPLATE_TYPES

logger = logging.getLogger(__name__)

_client = genai.Client(
    vertexai=True,
    project=Config.GOOGLE_CLOUD_PROJECT,
    location=Config.VERTEX_AI_LOCATION,
)

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


def plan_presentation(context: dict, slide_range: str) -> list[dict]:
    """
    Call Gemini to produce a slide plan.

    Args:
        context: dict with lesson data:
            - lesson_name (str)
            - subject (str)
            - grade (str)
            - objectives (list[str])
            - covered_concepts (list[{name, explanation}])
            - missing_concepts (list[{name, definition, importance}])
            - textbook_key_topics (list[{name, source}])
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

    covered = context.get("covered_concepts", [])
    missing = context.get("missing_concepts", [])
    topics = context.get("textbook_key_topics", [])
    objectives = context.get("objectives", [])
    activities = context.get("activities", [])

    user_prompt = f"""
Thông tin bài học:
- Tên bài: {context.get("lesson_name", "Không rõ")}
- Môn: {context.get("subject", "")} Lớp {context.get("grade", "")}

Mục tiêu bài học:
{json.dumps(objectives, ensure_ascii=False, indent=2)}

Chủ đề chính từ SGK:
{json.dumps([t.get("name") for t in topics], ensure_ascii=False, indent=2)}

Khái niệm đã phủ trong giáo án:
{json.dumps([c.get("name") for c in covered], ensure_ascii=False, indent=2)}

Khái niệm còn thiếu (quan trọng cần bổ sung):
{json.dumps([{"name": c.get("name"), "importance": c.get("importance")} for c in missing], ensure_ascii=False, indent=2)}

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

    response = _client.models.generate_content(
        model=Config.GEMINI_MODEL,
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=_SYSTEM_PROMPT,
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

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
