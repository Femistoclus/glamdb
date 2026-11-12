import unittest

from tools.monitoring.extend_checks import extend


class LocalProfileTests(unittest.TestCase):
    def test_source_profile_preserves_base_and_explicitly_overrides(self):
        base = {"selectors": {"node": "linux"}, "scope": {"cluster": "local"},
                "checks": [{"id": "cpu", "warn": 0.8, "alarm": 0.95}]}
        profile = {"selectors": {"mac": "darwin"},
                   "checks": [{"id": "logs", "warn": 0.5, "alarm": 1}],
                   "overrides": {"cpu": {"scope": {"node": "vm"}}}}
        result = extend(base, profile)
        self.assertEqual(result["selectors"], {"node": "linux", "mac": "darwin"})
        self.assertEqual(result["scope"], base["scope"])
        self.assertEqual(result["checks"], [dict(base["checks"][0], scope={"node": "vm"}),
                                            profile["checks"][0]])
        self.assertNotIn("scope", base["checks"][0])
        result["checks"][1]["warn"] = 3
        self.assertEqual(profile["checks"][0]["warn"], 0.5)

    def test_conflicting_inventory_is_rejected(self):
        base = {"selectors": {}, "checks": [{"id": "cpu"}]}
        for profile in ({"checks": [{"id": "cpu"}]},
                        {"overrides": {"missing": {"warn": 1}}},
                        {"overrides": {"cpu": {"id": "other"}}}):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                extend(base, profile)


if __name__ == "__main__":
    unittest.main()
