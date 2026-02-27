"""
Quick smoke test – verify Neo4j Aura is reachable and responding.

Run from the project root:
    python test/test_neo4j.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────────────
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)

NEO4J_URI = os.getenv("NEO4J_URI", "")
NEO4J_USER = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")


# ── Pre-flight checks ───────────────────────────────────────────────────────
def preflight():
    errors = []
    if not NEO4J_URI:
        errors.append("NEO4J_URI is not set in .env")
    if not NEO4J_PASSWORD:
        errors.append("NEO4J_PASSWORD is not set in .env")
    if errors:
        print("❌ Pre-flight failed:")
        for e in errors:
            print(f"   • {e}")
        sys.exit(1)
    print("✅ Pre-flight passed")
    print(f"   URI:  {NEO4J_URI}")
    print(f"   User: {NEO4J_USER}")
    print(f"   Pass: {NEO4J_PASSWORD}")


# ── Test 1: Verify connectivity ─────────────────────────────────────────────
def test_connectivity():
    from neo4j import GraphDatabase

    print("\n🔌 Test 1: Verify connectivity...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        driver.verify_connectivity()
        print("   ✅ Connected successfully!")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        sys.exit(1)
    finally:
        driver.close()


# ── Test 2: Run a simple query ───────────────────────────────────────────────
def test_simple_query():
    from neo4j import GraphDatabase

    print("\n📋 Test 2: Run a simple Cypher query...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 AS value")
            record = result.single()
            assert record["value"] == 1, "Unexpected query result"
            print("   ✅ Query executed: RETURN 1 → OK")
    except Exception as e:
        print(f"   ❌ Query failed: {e}")
        sys.exit(1)
    finally:
        driver.close()


# ── Test 3: Check server info ────────────────────────────────────────────────
def test_server_info():
    from neo4j import GraphDatabase

    print("\n🖥️  Test 3: Server info...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        info = driver.get_server_info()
        print(f"   Address:  {info.address}")
        print(f"   Agent:    {info.agent}")
        print(f"   Protocol: {info.protocol_version}")
        print("   ✅ Server info retrieved")
    except Exception as e:
        print(f"   ❌ Failed to get server info: {e}")
        sys.exit(1)
    finally:
        driver.close()


# ── Test 4: Count existing nodes ─────────────────────────────────────────────
def test_node_count():
    from neo4j import GraphDatabase

    print("\n📊 Test 4: Count existing nodes...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            result = session.run(
                "MATCH (n) RETURN labels(n) AS label, count(n) AS count "
                "ORDER BY count DESC LIMIT 10"
            )
            records = list(result)
            if records:
                print("   Node counts:")
                for r in records:
                    print(f"     {r['label']}: {r['count']}")
            else:
                print("   (database is empty — 0 nodes)")
            print("   ✅ Node count query OK")
    except Exception as e:
        print(f"   ❌ Query failed: {e}")
        sys.exit(1)
    finally:
        driver.close()


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Neo4j Connection Test")
    print("=" * 60)

    preflight()
    test_connectivity()
    test_simple_query()
    test_server_info()
    test_node_count()

    print("\n" + "=" * 60)
    print("All tests passed! ✅")
    print("=" * 60)
