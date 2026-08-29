#!/usr/bin/env python3
"""Workflow contract tests that run without GitHub Actions or external packages."""

import os
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "release.yml")
TOOL = os.path.join(ROOT, "scripts", "release_tool.py")


class TestReleaseWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(WORKFLOW, encoding="utf-8") as f:
            cls.workflow = f.read()

    def test_recovery_variables_are_step_local(self):
        publish = self.workflow.split("- name: Publish GitHub release", 1)[1]
        publish = publish.split("- name: Update catalog.json", 1)[0]
        self.assertIn("TOOL_SCRIPT:", publish)
        self.assertIn("RELEASE_DIR:", publish)
        self.assertIn('verify-existing-assets "$CHECK_DIR" "$RELEASE_DIR"', publish)

    def test_recovery_checks_deterministic_provenance(self):
        self.assertIn('COMMIT_TIME="$(git show -s --format=%cI "$GITHUB_SHA")"', self.workflow)
        self.assertIn('"$COMMIT_TIME"', self.workflow)
        with open(TOOL, encoding="utf-8") as f:
            tool = f.read()
        self.assertNotIn('datetime.now(', tool)

    def test_exact_asset_set_and_recovery(self):
        with open(TOOL, encoding="utf-8") as f:
            tool = f.read()
        with open(os.path.join(ROOT, "scripts", "test_release_tool.py"), encoding="utf-8") as f:
            tests = f.read()
        self.assertIn('REQUIRED_ASSETS = [', tool)
        self.assertIn("test_verify_asset_set_extra_asset_rejected", tests)
        self.assertIn("test_verify_asset_set_missing_asset_rejected", tests)

    def test_retry_exhaustion_fails(self):
        self.assertRegex(self.workflow, r'if \[ "\$PUSH_COMPLETED" != "true" \]; then\s+echo "::error::Failed to update catalog')

    def test_attestation_and_permissions(self):
        self.assertIn("id-token: write", self.workflow)
        self.assertIn("attestations: write", self.workflow)
        self.assertIn("actions/attest-build-provenance@", self.workflow)

    def test_pinned_toolchain_and_concurrency(self):
        self.assertIn('GO_VERSION: "1.26.0"', self.workflow)
        self.assertIn("group: release-pipeline", self.workflow)


if __name__ == "__main__":
    unittest.main()
