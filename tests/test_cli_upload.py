import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from cirro.cli.cli import run
from cirro.cli.interactive.upload_args import ask_tags


def _named(id_, name):
    item = MagicMock(id=id_)
    item.name = name
    return item


class TestUploadTags(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.TemporaryDirectory()
        Path(self.data_dir.name, "sample.fastq.gz").write_text("reads")

        self.cirro = MagicMock()
        self.cirro.projects.list.return_value = [_named("project-1", "Test Project")]
        process = _named("process-1", "Paired DNAseq (FASTQ)")
        self.cirro.processes.list.return_value = [process]
        self.cirro.processes.get.return_value = process

    def tearDown(self):
        self.data_dir.cleanup()

    def _upload(self, *extra_args):
        args = [
            "upload",
            "--project", "Test Project",
            "--data-type", "Paired DNAseq (FASTQ)",
            "--name", "test",
            "--data-directory", self.data_dir.name,
            *extra_args
        ]
        with patch("cirro.cli.controller._init_cirro_client", return_value=self.cirro):
            result = CliRunner().invoke(run, args, catch_exceptions=False)
        self.assertEqual(result.exit_code, 0, result.output)
        return self.cirro.datasets.create.call_args.kwargs["upload_request"]

    def test_tags_passed_to_create_request(self):
        request = self._upload("--tag", "microbiome-portal: metaphlan", "--tag", "study-x")
        tags = request.to_dict()["tags"]
        self.assertEqual([tag["value"] for tag in tags], ["microbiome-portal: metaphlan", "study-x"])
        # Tags are keyless; `key` is deprecated
        self.assertTrue(all("key" not in tag for tag in tags))

    def test_no_tags(self):
        request = self._upload()
        self.assertEqual(request.tags, [])


class TestAskTags(unittest.TestCase):
    def _ask(self, answer, input_value=None):
        with patch("cirro.cli.interactive.upload_args.prompt_wrapper", return_value={"tags": answer}) as prompt:
            tags = ask_tags(input_value)
        return tags, prompt.call_args.args[0]["default"]

    def test_splits_and_strips(self):
        tags, _ = self._ask(" microbiome-portal: wgs-reads , study-x,, ")
        self.assertEqual(tags, ["microbiome-portal: wgs-reads", "study-x"])

    def test_blank_answer_gives_no_tags(self):
        tags, default = self._ask("")
        self.assertEqual(tags, [])
        self.assertEqual(default, "")

    def test_defaults_to_command_line_tags(self):
        _, default = self._ask("a, b", input_value=("a", "b"))
        self.assertEqual(default, "a, b")
