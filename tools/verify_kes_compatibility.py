#!/usr/bin/env python3
"""Static compatibility gate for the KES-owned Flink/Spark connector fork.

No external Maven dependencies; this is NOT a runtime integration test.
"""
import re
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KIND = "flink" if (ROOT / "flink-doris-connector").exists() else "spark"
POM_ROOT = ROOT / (KIND + "-doris-connector")
UPSTREAM_VERSION = "26.3.0" if KIND == "flink" else "26.1.0"
KES_VERSION = UPSTREAM_VERSION + "-kes.1"
SERVICE = ("META-INF/services/org.apache.flink.table.factories.Factory"
           if KIND == "flink" else
           "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister")


class KesCompatibilityGate(unittest.TestCase):
    def test_maven_coordinates_and_external_thrift(self):
        poms = list(POM_ROOT.rglob("pom.xml"))
        self.assertEqual(5 if KIND == "flink" else 12, len(poms))
        for pom in poms:
            xml = pom.read_text(encoding="utf-8")
            ET.fromstring(xml)
            self.assertIn(KES_VERSION, xml, str(pom))
            self.assertNotIn("<artifactId>" + KIND + "-doris-connector", xml, str(pom))
            # Do not invent replacement coordinates for the upstream dependency.
            for m in re.finditer(
                    r"<groupId>org\.apache\.doris</groupId>\s*<artifactId>([^<]+)</artifactId>",
                    xml):
                self.assertEqual("thrift-service", m.group(1), str(pom))
        dep_pom = (POM_ROOT / ("flink-doris-connector-base" if KIND == "flink"
                             else "spark-doris-connector-base") / "pom.xml")
        self.assertIn("<artifactId>thrift-service</artifactId>",
                      dep_pom.read_text(encoding="utf-8"))

    def test_service_discovery_keeps_old_and_new(self):
        modules = (["flink-doris-connector-flink1", "flink-doris-connector-flink2"]
                   if KIND == "flink" else
                   ["spark-doris-connector-spark-2"] +
                   ["spark-doris-connector-spark-" + v
                    for v in ("3.1", "3.2", "3.3", "3.4", "3.5", "4.1")])
        for module in modules:
            f = POM_ROOT / module / "src/main/resources" / SERVICE
            x = f.read_text(encoding="utf-8")
            entries = [line for line in x.splitlines()
                       if line.strip() and not line.startswith("#")]
            self.assertEqual(4 if KIND == "flink" else 2, len(entries), str(f))
            self.assertEqual(len(set(entries)), len(entries), str(f))
            self.assertNotIn("kingbasees", x.lower(), str(f))
            self.assertTrue(any("Doris" in c for c in entries), str(f))
            self.assertTrue(any("Kes" in c for c in entries), str(f))
            for class_name in entries:
                java_name = class_name.replace(".", "/")
                java_file = POM_ROOT / module / "src/main/java" / (java_name + ".java")
                scala_file = POM_ROOT / module / "src/main/scala" / (java_name + ".scala")
                self.assertTrue(java_file.is_file() or scala_file.is_file(), class_name)

    def test_kes_alias_and_legacy_implementation_contract(self):
        if KIND == "flink":
            files = list(POM_ROOT.glob("flink-doris-connector-flink*/src/main/java/**/Kes*Factory.java"))
            # Java catalog and table aliases in each of two supported variants.
            self.assertEqual(4, len(files))
            for f in files:
                text = f.read_text(encoding="utf-8")
                self.assertIn('return "kes";', text, str(f))
            main = (POM_ROOT / "flink-doris-connector-base/src/main/java"
                    "/org/apache/doris/flink/sink/writer/LoadConstants.java")
            self.assertIn("__DORIS_DELETE_SIGN__", main.read_text(encoding="utf-8"))
        else:
            files = list(POM_ROOT.glob("spark-doris-connector-spark-*/src/main/scala/**/Kes*scala"))
            self.assertEqual(7, len(files))
            for f in files:
                self.assertIn('shortName(): String = "kes"', f.read_text(encoding="utf-8"))
            config = (POM_ROOT / "spark-doris-connector-base/src/main/java"
                      "/org/apache/doris/spark/config/DorisConfig.java").read_text(encoding="utf-8")
            self.assertIn('key.startsWith("kes.")', config)
            self.assertIn('putIfAbsent(normalizedKey, value)', config)
            self.assertIn("Conflicting connector option aliases", config)

    def test_packaging_script_expects_kes_jar(self):
        script = (POM_ROOT / "build.sh").read_text(encoding="utf-8")
        self.assertIn('grep "' + KIND + '-kes-"', script)
        self.assertIn(KIND + "-doris-connector", script)


if __name__ == "__main__":
    unittest.main(verbosity=2)
