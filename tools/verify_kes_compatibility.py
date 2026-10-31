#!/usr/bin/env python3
"""Static KES-only Spark connector gate; not a cluster test."""
import re
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "spark-kes-connector"
MODULES = tuple("spark-kes-connector-spark-" + x
                for x in ("2", "3.1", "3.2", "3.3", "3.4", "3.5", "4.1"))
VERSION = "26.1.0-kes.1"
SERVICE = "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"


class KesOnlyIdentityGate(unittest.TestCase):
    def test_maven_coordinates_keep_external_thrift(self):
        poms = list(PROJECT.rglob("pom.xml"))
        self.assertEqual(12, len(poms))
        for pom in poms:
            data = pom.read_text(encoding="utf-8")
            ET.fromstring(data)
            self.assertIn(VERSION, data, str(pom))
            self.assertNotIn("<artifactId>spark-doris-connector", data)
            for found in re.finditer(
                    r"<groupId>org\.apache\.doris</groupId>\s*<artifactId>([^<]+)</artifactId>",
                    data):
                self.assertEqual("thrift-service", found.group(1), str(pom))
        self.assertIn("<artifactId>thrift-service</artifactId>",
                      (PROJECT / "spark-kes-connector-base/pom.xml").read_text())

    def test_one_kes_only_data_source_per_supported_version(self):
        for module in MODULES:
            service = PROJECT / module / "src/main/resources" / SERVICE
            entries = [s.strip() for s in service.read_text().splitlines()
                       if s.strip() and not s.startswith("#")]
            suffix = "sql.KesSourceProvider" if module.endswith("-2") else "sql.sources.KesDataSource"
            expected = "com.kingbase.kes.spark." + suffix
            self.assertEqual([expected], entries, str(service))
            src = PROJECT / module / "src/main/scala" / (expected.replace(".", "/") + ".scala")
            self.assertTrue(src.is_file(), str(src))

    def test_no_old_package_or_alias_layer(self):
        base = PROJECT / "spark-kes-connector-base/src/main/java/com/kingbase/kes/spark"
        self.assertTrue(base.is_dir())
        self.assertFalse((PROJECT / "spark-kes-connector-base/src/main/java/org/apache/doris").exists())
        cfg = (base / "config/KesConfig.java").read_text()
        self.assertIn("Only kes.* connector options are supported", cfg)
        self.assertIn('key.startsWith("kes.")', cfg)
        self.assertNotIn('normalizedKey = "doris."', cfg)

    def test_kes_packaging_uses_kes_modules(self):
        script = (PROJECT / "build.sh").read_text()
        self.assertIn("spark-kes-connector-spark-", script)
        self.assertIn("spark-kes-", script)
        self.assertFalse((ROOT / "spark-doris-connector/pom.xml").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
