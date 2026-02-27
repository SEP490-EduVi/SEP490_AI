"""
Geography entity generator for Vietnamese SGK Địa Lí.

Graph: Book → Part → Chapter → Lesson → Section → Concept | Location | Figure
Structural IDs prefixed with book_id; knowledge nodes shared across books.
"""

import logging

from neo4j import Session

from entity_generator.base import (
    BaseEntityGenerator,
    NodeProperty,
    NodeType,
    RelationshipType,
)

logger = logging.getLogger(__name__)

# ── Prompts ──

_SYSTEM_PROMPT = (
    "Bạn là chuyên gia phân tích Sách Giáo Khoa Địa Lí Việt Nam.\n"
    "Nhiệm vụ: Phân tích nội dung sách giáo khoa và trích xuất cấu trúc "
    "bao gồm Phần, Chương, Bài, Mục, cùng các thực thể tri thức (Khái niệm, "
    "Địa danh, Hình ảnh/Bản đồ/Bảng).\n"
    "\n"
    "CẤU TRÚC SÁCH: Sách gồm nhiều PHẦN (Part), mỗi Phần chứa nhiều CHƯƠNG (Chapter), "
    "mỗi Chương chứa nhiều BÀI (Lesson), mỗi Bài chứa nhiều MỤC (Section).\n"
    "Nếu Bài nằm trực tiếp dưới Phần mà KHÔNG thuộc Chương nào, vẫn đặt nó dưới Phần đó "
    "(mảng chapters của Phần sẽ rỗng hoặc Bài đặt ở lessons trực tiếp).\n"
    "\n"
    "NGUYÊN TẮC TRÍCH XUẤT CONCEPT (QUAN TRỌNG):\n"
    "Phân biệt rõ: khái niệm được DẠY/GIẢI THÍCH trong bài vs. từ chỉ được NÊU TÊN/LIỆT KÊ.\n"
    "- CHỈ trích xuất khái niệm mà bài đang DẠY, ĐỊNH NGHĨA hoặc GIẢI THÍCH chi tiết.\n"
    "- KHÔNG trích xuất từ chỉ được NHẮC QUA, LIỆT KÊ trong danh sách, hoặc dùng làm ngữ cảnh.\n"
    "- Phép thử: \"Học sinh có cần HỌC NGHĨA của thuật ngữ này từ BÀI NÀY không?\"\n"
    "  Nếu KHÔNG → bỏ qua.\n"
    "\n"
    "QUY TẮC CHUNG:\n"
    "- Giữ nguyên tiếng Việt, KHÔNG dịch sang tiếng Anh.\n"
    "- Trả về JSON thuần túy, KHÔNG markdown code fences.\n"
    "- Mỗi Section phải chứa TOÀN BỘ nội dung giảng dạy gốc trong trường 'content'.\n"
    "- LOẠI BỎ khỏi content: câu hỏi ôn tập (dòng bắt đầu bằng ?, \"Đọc thông tin\", "
    "\"Dựa vào\", \"Hãy cho biết\"), hộp \"KẾT NỐI TRI THỨC VỚI CUỘC SỐNG\", "
    "và các chỉ dẫn như \"Quan sát hình...\".\n"
    "- Location: CHỈ trích xuất ĐỊA DANH CỤ THỂ là tên riêng (Proper Noun). "
    "KHÔNG lấy từ chung chung như 'thế giới', 'các nước', 'khu vực'.\n"
)

_USER_PROMPT_TEMPLATE = """Phân tích nội dung Sách Giáo Khoa Địa Lí sau đây:

**Môn học:** {subject}
**Lớp:** {grade}

**Nội dung:**
{text}

---

Hãy trích xuất và trả về JSON với cấu trúc sau:

{{
  "subject": "{subject}",
  "grade": "{grade}",
  "parts": [
    {{
      "id": "P1",
      "name": "Phần một. TÊN PHẦN",
      "order": 1,
      "chapters": [
        {{
          "id": "C1",
          "name": "Chương 1. TÊN CHƯƠNG",
          "order": 1,
          "lessons": [
            {{
              "id": "L2",
              "name": "Bài 2: Tên bài học",
              "order": 2,
              "sections": [
                {{
                  "id": "Sec_2.1",
                  "heading": "1. Tiêu đề mục",
                  "content": "Toàn bộ văn bản gốc của mục này...",
                  "page": 7,
                  "concepts": [
                    {{
                      "name": "Tên khái niệm",
                      "definition": "Câu định nghĩa ngắn gọn hoặc null"
                    }}
                  ],
                  "locations": [
                    {{
                      "name": "Tên địa danh cụ thể",
                      "type": "Loại (Quốc gia/Núi/Sông/Biển/...)"
                    }}
                  ],
                  "figures": [
                    {{
                      "id": "Fig_2.1",
                      "caption": "Hình 2.1. Mô tả hình"
                    }}
                  ]
                }}
              ]
            }}
          ]
        }}
      ],
      "lessons": [
        {{
          "id": "L1",
          "name": "Bài 1: Tên bài (nằm trực tiếp dưới Phần, không thuộc Chương nào)",
          "order": 1,
          "sections": []
        }}
      ]
    }}
  ]
}}

QUY TẮC TRÍCH XUẤT:

1. PHẦN (Part):
   - id: Mã ngắn gọn (P1, P2, P3, ...).
   - name: Tên đầy đủ (VD: "Phần một. MỘT SỐ VẤN ĐỀ CHUNG").
   - order: Số thứ tự phần, BẮT ĐẦU từ 1.
   - chapters: Mảng các Chương thuộc Phần này.
   - lessons: Mảng các Bài nằm TRỰC TIẾP dưới Phần mà KHÔNG thuộc Chương nào.
     Nếu tất cả Bài đều thuộc Chương, mảng này rỗng [].

2. CHƯƠNG (Chapter):
   - id: Mã ngắn gọn (C1, C2, ...). Đánh số theo sách (Chương 1 → C1).
   - name: Tên đầy đủ (VD: "Chương 1. SỬ DỤNG BẢN ĐỒ").
   - order: Số thứ tự chương THEO SÁCH (1, 2, 3, ...).

3. BÀI HỌC (Lesson):
   - id: Mã ngắn gọn (L1, L9, ...). Đánh số theo sách (Bài 1 → L1).
   - name: Tên đầy đủ bao gồm số bài.
   - order: Số thứ tự bài THEO SÁCH.

4. MỤC (Section) — QUAN TRỌNG NHẤT:
   - id: Theo format "Sec_<số bài>.<số mục>" (VD: "Sec_9.1").
   - heading: Tiêu đề mục. Nếu có đoạn mở đầu TRƯỚC mục "1." đầu tiên, tạo một Section
     riêng với heading = "Mở đầu".
   - content: SAO CHÉP phần nội dung BÀI GIẢNG của mục đó.
     + GIỮ LẠI: nội dung giảng dạy, giải thích, ví dụ minh hoạ.
     + LOẠI BỎ: câu hỏi ôn tập (?, "Đọc thông tin", "Dựa vào", "Hãy cho biết"),
       hộp "KẾT NỐI TRI THỨC VỚI CUỘC SỐNG", chỉ dẫn "Quan sát hình...",
       và phần "Câu hỏi và bài tập" cuối bài.
   - page: Số trang trong PDF (nếu có thông tin "--- Page X ---").

5. KHÁI NIỆM (Concept) — CHẤT LƯỢNG LÀ TRÊN HẾT:
   **Phép thử bắt buộc:** Trước khi thêm một concept, tự hỏi:
   "Thuật ngữ này có được bài DẠY, ĐỊNH NGHĨA hoặc GIẢI THÍCH CHI TIẾT không?"
   - Nếu CÓ → trích xuất, kèm definition.
   - Nếu CHỈ được nhắc tên / liệt kê trong danh sách / dùng làm ngữ cảnh → BỎ QUA.

   Ví dụ áp dụng phép thử:
   - Bài viết: "...gồm địa lí tự nhiên và địa lí kinh tế – xã hội" → chỉ LIỆT KÊ → BỎ QUA.
   - Bài viết: "Phong hóa là quá trình phá hủy đá..." → ĐỊNH NGHĨA → TRÍCH XUẤT.
   - Bài viết: "kiến thức về địa hình, khí hậu, thuỷ văn" → chỉ LIỆT KÊ → BỎ QUA.
   - Bài viết: "Khí quyển là lớp không khí bao quanh Trái Đất..." → GIẢI THÍCH → TRÍCH XUẤT.

   Danh sách KHÔNG BAO GIỜ trích xuất (kể cả khi xuất hiện trong bài):
   - Tên lĩnh vực chung: "Khoa học tự nhiên", "Khoa học xã hội", "Khoa học địa lí".
   - Danh từ phổ thông: "Kinh tế", "Thương mại", "Dịch vụ", "Công nghiệp",
     "Nông nghiệp", "Môi trường", "Đất đai", "Sinh vật", "Giáo dục".
   - Tên nghề: "Kĩ sư trắc địa", "Hướng dẫn viên du lịch", v.v.

   definition: BẮT BUỘC trích câu định nghĩa/giải thích từ nội dung bài. Nếu bài không
   đưa ra định nghĩa rõ ràng cho thuật ngữ đó thì KHÔNG THÊM concept này.
   Ngoại lệ duy nhất: thuật ngữ rất chuyên ngành mà học sinh chắc chắn cần ghi nhớ
   (VD: "Thạch quyển", "Sinh quyển") thì được phép để definition = null.

6. ĐỊA DANH (Location):
   - CHỈ trích xuất các ĐỊA DANH CỤ THỂ là TÊN RIÊNG (Proper Noun).
   - Bao gồm: tên quốc gia, thành phố, châu lục, đại dương, sông, núi, đồng bằng, biển, hồ, sa mạc cụ thể.
   - VD đúng: "Việt Nam", "Dãy Himalaya", "Sông Hồng", "Biển Đông", "Châu Á", "Thái Bình Dương".
   - TUYỆT ĐỐI KHÔNG lấy các từ chung chung: "Thế giới", "Toàn cầu", "Các nước", "Các khu vực", "Địa phương", "Lục địa" (không kèm tên cụ thể).
   - type: Phân loại (Quốc gia, Châu lục, Núi, Sông, Biển, Đồng bằng, Đại dương, Sa mạc, Hồ, Thành phố, ...).

7. HÌNH ẢNH (Figure):
   - Các hình, bản đồ, bảng biểu được đề cập trong nội dung giảng dạy.
   - id: Theo format "Fig_<số bài>.<số hình>" (VD: "Fig_9.1").
   - caption: Tên đầy đủ của hình (VD: "Hình 9.1. Sự phân bố nhiệt độ...").

8. Nếu không xác định được phần/chương/bài, hãy suy luận từ tiêu đề và nội dung.
9. Ưu tiên ÍT nhưng CHÍNH XÁC hơn NHIỀU nhưng SAI.
   Một Section GIỚI THIỆU (như Bài 1 giới thiệu môn học) có thể có 0-2 Concept hoặc KHÔNG có concept nào — đó là bình thường.
   Một Section dạy kiến thức mới (như Bài 6 về Thạch quyển) nên có 2-5 Concept kèm definition.

Trả về JSON thuần túy.
"""

# ── Generator class ──


class GeographyEntityGenerator(BaseEntityGenerator):
    """Vietnamese Geography textbook entity generator."""

    def get_skip_pages(self) -> tuple[int, int]:
        """Skip first 5 pages (cover/TOC) and last 5 pages (appendix/index)."""
        return (5, 5)

    def get_node_types(self) -> list[NodeType]:
        return [
            NodeType(
                label="Book",
                description="Sách giáo khoa – nút gốc cho mỗi cuốn sách.",
                properties=[
                    NodeProperty("id", "String", "Mã định danh sách (VD: dia_li_10)"),
                    NodeProperty("subject", "String", "Tên môn học"),
                    NodeProperty("grade", "String", "Lớp/Khối"),
                ],
            ),
            NodeType(
                label="Part",
                description="Phần – nhóm cấp cao nhất dưới Sách.",
                properties=[
                    NodeProperty("id", "String", "Mã định danh (VD: dia_li_10_P1)"),
                    NodeProperty("name", "String", "Tên phần"),
                    NodeProperty("order", "Integer", "Số thứ tự phần"),
                ],
            ),
            NodeType(
                label="Chapter",
                description="Chương – nhóm các bài học trong một Phần.",
                properties=[
                    NodeProperty("id", "String", "Mã định danh (VD: dia_li_10_C1)"),
                    NodeProperty("name", "String", "Tên chương"),
                    NodeProperty("order", "Integer", "Số thứ tự"),
                ],
            ),
            NodeType(
                label="Lesson",
                description="Bài học cụ thể mà học sinh theo dõi.",
                properties=[
                    NodeProperty("id", "String", "Mã định danh (VD: dia_li_10_L9)"),
                    NodeProperty("name", "String", "Tên bài"),
                    NodeProperty("order", "Integer", "Số thứ tự bài"),
                ],
            ),
            NodeType(
                label="Section",
                description="Mục nội dung – thùng chứa văn bản gốc cho RAG.",
                properties=[
                    NodeProperty("id", "String", "Mã định danh (VD: dia_li_10_Sec_9.1)"),
                    NodeProperty("heading", "String", "Tiêu đề mục"),
                    NodeProperty("content", "String", "Toàn bộ văn bản của mục"),
                    NodeProperty("page", "Integer", "Trang trong PDF"),
                ],
            ),
            NodeType(
                label="Concept",
                description="Khái niệm/Thuật ngữ/Quá trình địa lí.",
                properties=[
                    NodeProperty("name", "String", "Tên khái niệm"),
                    NodeProperty(
                        "definition", "String",
                        "Câu định nghĩa ngắn gọn", required=False,
                    ),
                ],
            ),
            NodeType(
                label="Location",
                description="Địa danh – không gian địa lý thực tế.",
                properties=[
                    NodeProperty("name", "String", "Tên địa danh"),
                    NodeProperty(
                        "type", "String",
                        "Phân loại (Quốc gia, Núi, Sông, ...)", required=False,
                    ),
                ],
            ),
            NodeType(
                label="Figure",
                description="Hình ảnh/Bản đồ/Bảng trong sách.",
                properties=[
                    NodeProperty("id", "String", "Mã hình (VD: Fig_9.1)"),
                    NodeProperty("caption", "String", "Tên/mô tả hình"),
                ],
            ),
        ]

    def get_relationship_types(self) -> list[RelationshipType]:
        return [
            RelationshipType(
                "HAS", "Book", "Part",
                "Sách chứa các Phần.",
            ),
            RelationshipType(
                "HAS", "Part", "Chapter",
                "Phần chứa các Chương.",
            ),
            RelationshipType(
                "HAS", "Part", "Lesson",
                "Phần chứa Bài trực tiếp (không thuộc Chương nào).",
            ),
            RelationshipType(
                "HAS", "Chapter", "Lesson",
                "Chương chứa các Bài.",
            ),
            RelationshipType(
                "HAS", "Lesson", "Section",
                "Bài chứa các Mục (nơi chứa text).",
            ),
            RelationshipType(
                "MENTIONS", "Section", "Concept",
                "Đoạn văn nhắc đến/giải thích khái niệm này.",
            ),
            RelationshipType(
                "MENTIONS", "Section", "Location",
                "Đoạn văn lấy địa danh này làm ví dụ.",
            ),
            RelationshipType(
                "MENTIONS", "Section", "Figure",
                "Đoạn văn yêu cầu xem hình ảnh này.",
            ),
        ]

    # ── Prompts ──

    def get_system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def get_extraction_prompt(
        self, raw_text: str, subject: str, grade: str
    ) -> str:
        return _USER_PROMPT_TEMPLATE.format(
            subject=subject,
            grade=grade,
            text=raw_text,
        )

    # ── Response parsing ──

    def parse_response(self, raw_json: dict, book_id: str) -> dict:
        """Validate, normalise, and prefix IDs for single-DB isolation."""
        parts = raw_json.get("parts", [])
        if not parts:
            raise ValueError("LLM returned no parts.")

        raw_json["book_id"] = book_id

        for pt_idx, part in enumerate(parts):
            raw_id = part.get("id", f"P{pt_idx + 1}")
            part["id"] = f"{book_id}_{raw_id}"
            part.setdefault("name", f"Phần {pt_idx + 1}")
            part.setdefault("order", pt_idx + 1)

            # Chapters inside this Part
            for ch_idx, chapter in enumerate(part.get("chapters", [])):
                raw_id = chapter.get("id", f"C{ch_idx + 1}")
                chapter["id"] = f"{book_id}_{raw_id}"
                chapter.setdefault("name", f"Chương {ch_idx + 1}")
                chapter.setdefault("order", ch_idx + 1)

                for ls_idx, lesson in enumerate(chapter.get("lessons", [])):
                    self._normalise_lesson(lesson, ls_idx, book_id)

            # Lessons directly under Part (no chapter)
            for ls_idx, lesson in enumerate(part.get("lessons", [])):
                self._normalise_lesson(lesson, ls_idx, book_id)

        raw_json["parts"] = parts
        return raw_json

    # ── Internal helpers for parse_response ──

    @staticmethod
    def _normalise_lesson(lesson: dict, ls_idx: int, book_id: str) -> None:
        raw_id = lesson.get("id", f"L{ls_idx + 1}")
        lesson["id"] = f"{book_id}_{raw_id}"
        lesson.setdefault("name", f"Bài {ls_idx + 1}")
        lesson.setdefault("order", ls_idx + 1)

        for sec_idx, section in enumerate(lesson.get("sections", [])):
            raw_id = section.get(
                "id",
                f"Sec_{lesson['id'].replace(f'{book_id}_L', '')}.{sec_idx + 1}",
            )
            if not raw_id.startswith(book_id):
                section["id"] = f"{book_id}_{raw_id}"
            else:
                section["id"] = raw_id
            # setdefault won't replace explicit None, so use fallback
            if section.get("heading") is None:
                section["heading"] = f"Mục {sec_idx + 1}"
            if section.get("content") is None:
                section["content"] = ""
            if section.get("page") is None:
                section["page"] = 0
            section.setdefault("heading", f"Mục {sec_idx + 1}")
            section.setdefault("content", "")
            section.setdefault("page", 0)
            section.setdefault("concepts", [])
            section.setdefault("locations", [])
            section.setdefault("figures", [])

            for fig in section.get("figures", []):
                fig_raw = fig.get("id", f"Fig_{sec_idx + 1}")
                if not fig_raw.startswith(book_id):
                    fig["id"] = f"{book_id}_{fig_raw}"

    # ── Constraints ──

    def get_constraints(self) -> list[str]:
        return [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Book) REQUIRE b.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Part) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Lesson) REQUIRE l.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Section) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (f:Figure) REQUIRE f.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (co:Concept) REQUIRE co.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (lo:Location) REQUIRE lo.name IS UNIQUE",
        ]

    # ── Neo4j loading ──

    def load_to_neo4j(self, session: Session, data: dict) -> dict[str, int]:
        """Persist Book → Part → Chapter → Lesson → Section → knowledge nodes."""
        book_id = data.get("book_id", "")
        subject = data.get("subject", "")
        grade = data.get("grade", "")
        parts = data.get("parts", [])

        counts: dict[str, int] = {
            "books": 0,
            "parts": 0,
            "chapters": 0,
            "lessons": 0,
            "sections": 0,
            "concepts": 0,
            "locations": 0,
            "figures": 0,
        }

        # Book root node
        self._create_book(session, book_id, subject, grade)
        counts["books"] += 1

        for part in parts:
            self._create_part(session, part, book_id)
            counts["parts"] += 1

            # Chapters inside this Part
            for chapter in part.get("chapters", []):
                self._create_chapter(session, chapter, part["id"])
                counts["chapters"] += 1

                for lesson in chapter.get("lessons", []):
                    self._create_lesson(session, lesson, chapter["id"])
                    counts["lessons"] += 1
                    self._load_lesson_sections(session, lesson, counts)

            # Lessons directly under Part (no chapter)
            for lesson in part.get("lessons", []):
                self._create_lesson_under_part(session, lesson, part["id"])
                counts["lessons"] += 1
                self._load_lesson_sections(session, lesson, counts)

        logger.info(
            "Neo4j load complete: %s",
            ", ".join(f"{k}={v}" for k, v in counts.items()),
        )
        return counts

    def _load_lesson_sections(
        self, session: Session, lesson: dict, counts: dict[str, int]
    ) -> None:
        """Load sections and knowledge nodes for a single lesson."""
        for section in lesson.get("sections", []):
            self._create_section(session, section, lesson["id"])
            counts["sections"] += 1

            for concept in section.get("concepts", []):
                self._link_concept(session, section["id"], concept)
                counts["concepts"] += 1

            for location in section.get("locations", []):
                self._link_location(session, section["id"], location)
                counts["locations"] += 1

            for figure in section.get("figures", []):
                self._link_figure(session, section["id"], figure)
                counts["figures"] += 1

    # ── Cypher helpers ──

    @staticmethod
    def _create_book(
        session: Session, book_id: str, subject: str, grade: str
    ) -> None:
        session.run(
            """
            MERGE (b:Book {id: $id})
            ON CREATE SET
                b.subject = $subject,
                b.grade   = $grade
            ON MATCH SET
                b.subject = $subject,
                b.grade   = $grade
            """,
            id=book_id,
            subject=subject,
            grade=grade,
        )
        logger.info("Book: %s (%s / %s)", book_id, subject, grade)

    @staticmethod
    def _create_part(
        session: Session, part: dict, book_id: str
    ) -> None:
        session.run(
            """
            MATCH  (b:Book {id: $book_id})
            MERGE  (p:Part {id: $id})
            ON CREATE SET
                p.name    = $name,
                p.`order` = $order
            ON MATCH SET
                p.name    = $name,
                p.`order` = $order
            MERGE  (b)-[:HAS]->(p)
            """,
            book_id=book_id,
            id=part["id"],
            name=part["name"],
            order=part["order"],
        )
        logger.info("  Part: %s – %s", part["id"], part["name"])

    @staticmethod
    def _create_chapter(
        session: Session, chapter: dict, part_id: str
    ) -> None:
        session.run(
            """
            MATCH  (p:Part {id: $part_id})
            MERGE  (c:Chapter {id: $id})
            ON CREATE SET
                c.name    = $name,
                c.`order` = $order
            ON MATCH SET
                c.name    = $name,
                c.`order` = $order
            MERGE  (p)-[:HAS]->(c)
            """,
            part_id=part_id,
            id=chapter["id"],
            name=chapter["name"],
            order=chapter["order"],
        )
        logger.info("    Chapter: %s – %s", chapter["id"], chapter["name"])

    @staticmethod
    def _create_lesson(
        session: Session, lesson: dict, chapter_id: str
    ) -> None:
        session.run(
            """
            MATCH  (c:Chapter {id: $chapter_id})
            MERGE  (l:Lesson  {id: $id})
            ON CREATE SET
                l.name    = $name,
                l.`order` = $order
            ON MATCH SET
                l.name    = $name,
                l.`order` = $order
            MERGE  (c)-[:HAS]->(l)
            """,
            chapter_id=chapter_id,
            id=lesson["id"],
            name=lesson["name"],
            order=lesson["order"],
        )
        logger.info("      Lesson: %s – %s", lesson["id"], lesson["name"])

    @staticmethod
    def _create_lesson_under_part(
        session: Session, lesson: dict, part_id: str
    ) -> None:
        """Lesson that sits directly under a Part (no Chapter)."""
        session.run(
            """
            MATCH  (p:Part {id: $part_id})
            MERGE  (l:Lesson  {id: $id})
            ON CREATE SET
                l.name    = $name,
                l.`order` = $order
            ON MATCH SET
                l.name    = $name,
                l.`order` = $order
            MERGE  (p)-[:HAS]->(l)
            """,
            part_id=part_id,
            id=lesson["id"],
            name=lesson["name"],
            order=lesson["order"],
        )
        logger.info("    Lesson (Part-direct): %s – %s", lesson["id"], lesson["name"])

    @staticmethod
    def _create_section(
        session: Session, section: dict, lesson_id: str
    ) -> None:
        session.run(
            """
            MATCH (l:Lesson {id: $lesson_id})
            MERGE (s:Section {id: $id})
            ON CREATE SET
                s.heading  = $heading,
                s.content  = $content,
                s.page     = $page
            ON MATCH SET
                s.heading  = $heading,
                s.content  = $content,
                s.page     = $page
            MERGE (l)-[:HAS]->(s)
            """,
            lesson_id=lesson_id,
            id=section["id"],
            heading=section["heading"],
            content=section["content"],
            page=section["page"],
        )
        logger.info(
            "      Section: %s – %s", section["id"], section["heading"]
        )

    @staticmethod
    def _link_concept(
        session: Session, section_id: str, concept: dict
    ) -> None:
        name = concept.get("name", "").strip()
        if not name:
            return
        session.run(
            """
            MATCH (s:Section {id: $section_id})
            MERGE (co:Concept {name: $name})
            ON CREATE SET co.definition = $definition
            MERGE (s)-[:MENTIONS]->(co)
            """,
            section_id=section_id,
            name=name,
            definition=concept.get("definition", ""),
        )

    @staticmethod
    def _link_location(
        session: Session, section_id: str, location: dict
    ) -> None:
        name = location.get("name", "").strip()
        if not name:
            return
        session.run(
            """
            MATCH (s:Section {id: $section_id})
            MERGE (lo:Location {name: $name})
            ON CREATE SET lo.type = $type
            MERGE (s)-[:MENTIONS]->(lo)
            """,
            section_id=section_id,
            name=name,
            type=location.get("type", ""),
        )

    @staticmethod
    def _link_figure(
        session: Session, section_id: str, figure: dict
    ) -> None:
        fig_id = figure.get("id", "").strip()
        if not fig_id:
            return
        session.run(
            """
            MATCH (s:Section {id: $section_id})
            MERGE (f:Figure {id: $fig_id})
            ON CREATE SET f.caption = $caption
            MERGE (s)-[:MENTIONS]->(f)
            """,
            section_id=section_id,
            fig_id=fig_id,
            caption=figure.get("caption", ""),
        )
