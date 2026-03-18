"""
Neo4j loader — MERGE curriculum nodes and COVERS relationships.

Two operations:
  1. ingest(curriculum, subject_code) — MERGE all MonHoc/Lop/ChuDe/YeuCau nodes
     and their HAS/MENTIONS relationships. Fully idempotent.

  2. apply_mapping(mapping_path) — Read a mapping JSON file and MERGE
     (Lesson)-[:COVERS]->(ChuDe) relationships. Also idempotent.

Neo4j label isolation:
  Curriculum labels : MonHoc, Lop, ChuDe, YeuCau
  Textbook labels   : Book, Part, Chapter, Lesson, Section, Concept, Location, Figure
  No label overlap. The only cross-schema link is (Lesson)-[:COVERS]->(ChuDe).

ID convention:
  MonHoc : "{subject_code}_{cap_hoc_slug}_{year}"   e.g. "dia_li_thpt_2018"
  Lop    : "{mon_hoc_id}_{lop_slug}"                e.g. "dia_li_thpt_2018_lop_10"
  ChuDe  : "{lop_id}_CD{counter:02d}"               e.g. "dia_li_thpt_2018_lop_10_CD01"
  YeuCau : "{chu_de_id}_YC{counter:02d}"             e.g. "dia_li_thpt_2018_lop_10_CD01_YC01"
"""

import json
import logging
import re
from pathlib import Path

from neo4j import GraphDatabase

from config import Config
from parser import ParsedCurriculum

logger = logging.getLogger(__name__)


def _slug(text: str) -> str:
    """Convert Vietnamese text to a lowercase ASCII slug for use in IDs."""
    text = text.lower()
    # Common Vietnamese diacritic replacements (enough for grade names)
    replacements = {
        "ớ": "o", "ợ": "o", "ở": "o", "ỡ": "o", "ờ": "o",
        "ố": "o", "ộ": "o", "ổ": "o", "ỗ": "o", "ồ": "o",
        "ó": "o", "ò": "o", "ỏ": "o", "õ": "o", "ọ": "o",
        "ô": "o", "ơ": "o",
        "á": "a", "à": "a", "ả": "a", "ã": "a", "ạ": "a",
        "ắ": "a", "ặ": "a", "ẳ": "a", "ẵ": "a", "ằ": "a",
        "ấ": "a", "ậ": "a", "ẩ": "a", "ẫ": "a", "ầ": "a",
        "â": "a", "ă": "a",
        "é": "e", "è": "e", "ẻ": "e", "ẽ": "e", "ẹ": "e",
        "ế": "e", "ệ": "e", "ể": "e", "ễ": "e", "ề": "e",
        "ê": "e",
        "í": "i", "ì": "i", "ỉ": "i", "ĩ": "i", "ị": "i",
        "ú": "u", "ù": "u", "ủ": "u", "ũ": "u", "ụ": "u",
        "ứ": "u", "ự": "u", "ử": "u", "ữ": "u", "ừ": "u",
        "ư": "u",
        "ý": "y", "ỳ": "y", "ỷ": "y", "ỹ": "y", "ỵ": "y",
        "đ": "d",
    }
    for viet, ascii_char in replacements.items():
        text = text.replace(viet, ascii_char)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text


def _cap_hoc_slug(cap_hoc: str) -> str:
    """e.g. 'THPT' → 'thpt'"""
    return cap_hoc.lower().replace(" ", "_")


def _lop_slug(ten_lop: str) -> str:
    """e.g. 'Lớp 10' → 'lop_10'"""
    match = re.search(r"\d+", ten_lop)
    number = match.group(0) if match else _slug(ten_lop)
    return f"lop_{number}"


def ingest(curriculum: ParsedCurriculum, subject_code: str, curriculum_year: int | None = None) -> dict:
    """
    MERGE all curriculum nodes into Neo4j.

    Returns stats dict: {lop_count, chu_de_count, yeu_cau_count}
    """
    driver = GraphDatabase.driver(
        Config.NEO4J_URI,
        auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
    )

    cap_hoc_slug = _cap_hoc_slug(curriculum.cap_hoc)
    year_suffix = f"_{curriculum_year}" if curriculum_year else ""
    mon_hoc_id = f"{subject_code}_{cap_hoc_slug}{year_suffix}"

    lop_count = 0
    chu_de_count = 0
    yeu_cau_count = 0

    try:
        with driver.session() as session:
            # ── MonHoc ────────────────────────────────────────────────
            session.run(
                """
                MERGE (m:MonHoc {id: $id})
                SET m.ten_mon = $ten_mon,
                    m.cap_hoc = $cap_hoc,
                    m.curriculum_year = $curriculum_year
                """,
                id=mon_hoc_id,
                ten_mon=curriculum.ten_mon,
                cap_hoc=curriculum.cap_hoc,
                curriculum_year=curriculum_year,
            )
            logger.info("MERGED MonHoc: %s", mon_hoc_id)

            for lop in curriculum.lop_list:
                lop_id = f"{mon_hoc_id}_{_lop_slug(lop.ten_lop)}"

                # ── Lop ───────────────────────────────────────────────
                session.run(
                    """
                    MERGE (l:Lop {id: $id})
                    SET l.ten_lop = $ten_lop
                    WITH l
                    MATCH (m:MonHoc {id: $mon_hoc_id})
                    MERGE (m)-[:HAS]->(l)
                    """,
                    id=lop_id,
                    ten_lop=lop.ten_lop,
                    mon_hoc_id=mon_hoc_id,
                )
                lop_count += 1
                logger.info("  MERGED Lop: %s", lop_id)

                for cd_idx, chu_de in enumerate(lop.chu_de_list, start=1):
                    chu_de_id = f"{lop_id}_CD{cd_idx:02d}"

                    # ── ChuDe ─────────────────────────────────────────
                    session.run(
                        """
                        MERGE (cd:ChuDe {id: $id})
                        SET cd.ten_chu_de = $ten_chu_de,
                            cd.phan_mon   = $phan_mon
                        WITH cd
                        MATCH (l:Lop {id: $lop_id})
                        MERGE (l)-[:HAS]->(cd)
                        """,
                        id=chu_de_id,
                        ten_chu_de=chu_de.ten_chu_de,
                        phan_mon=chu_de.phan_mon,
                        lop_id=lop_id,
                    )
                    chu_de_count += 1
                    logger.info("    MERGED ChuDe: %s (%s)", chu_de_id, chu_de.ten_chu_de)

                    for yc_idx, yeu_cau in enumerate(chu_de.yeu_cau_list, start=1):
                        yeu_cau_id = f"{chu_de_id}_YC{yc_idx:02d}"

                        # ── YeuCau ────────────────────────────────────
                        session.run(
                            """
                            MERGE (yc:YeuCau {id: $id})
                            SET yc.noi_dung   = $noi_dung,
                                yc.tieu_chuan = $tieu_chuan
                            WITH yc
                            MATCH (cd:ChuDe {id: $chu_de_id})
                            MERGE (cd)-[:MENTIONS]->(yc)
                            """,
                            id=yeu_cau_id,
                            noi_dung=yeu_cau.noi_dung,
                            tieu_chuan=yeu_cau.tieu_chuan,
                            chu_de_id=chu_de_id,
                        )
                        yeu_cau_count += 1

    finally:
        driver.close()

    stats = {
        "lop_count": lop_count,
        "chu_de_count": chu_de_count,
        "yeu_cau_count": yeu_cau_count,
    }
    logger.info("Ingest complete: %s", stats)
    return stats


def apply_mapping(mapping_path: str) -> int:
    """
    Read a mapping JSON file and MERGE (Lesson)-[:COVERS]->(ChuDe) relationships.

    Mapping file format:
    {
        "dia_li_lop_10_L1": ["dia_li_thpt_lop_10_CD01"],
        "dia_li_lop_10_L3": ["dia_li_thpt_lop_10_CD03", "dia_li_thpt_lop_10_CD04"]
    }

    Returns the number of COVERS relationships merged.
    """
    path = Path(mapping_path)
    if not path.exists():
        logger.warning("Mapping file not found: %s — skipping COVERS links", mapping_path)
        return 0

    with open(path, encoding="utf-8") as f:
        mapping: dict[str, list[str]] = json.load(f)

    driver = GraphDatabase.driver(
        Config.NEO4J_URI,
        auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
    )

    covers_count = 0
    try:
        with driver.session() as session:
            for lesson_id, chu_de_ids in mapping.items():
                for chu_de_id in chu_de_ids:
                    result = session.run(
                        """
                        MATCH (lesson:Lesson {id: $lesson_id})
                        MATCH (cd:ChuDe {id: $chu_de_id})
                        MERGE (lesson)-[:COVERS]->(cd)
                        RETURN lesson.id AS lid, cd.id AS cdid
                        """,
                        lesson_id=lesson_id,
                        chu_de_id=chu_de_id,
                    )
                    record = result.single()
                    if record:
                        covers_count += 1
                        logger.info(
                            "MERGED COVERS: (%s)-[:COVERS]->(%s)",
                            lesson_id, chu_de_id,
                        )
                    else:
                        logger.warning(
                            "COVERS skipped — node not found: Lesson=%s or ChuDe=%s",
                            lesson_id, chu_de_id,
                        )
    finally:
        driver.close()

    logger.info("Applied %d COVERS relationships from %s", covers_count, mapping_path)
    return covers_count
