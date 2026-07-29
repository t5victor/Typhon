import ast
from pathlib import Path
import unittest


class IntentionLedNaming(unittest.TestCase):
    def test_command_and_event_titles_do_not_smuggle_crud_into_the_domain(self) -> None:
        root = Path(__file__).parents[2]
        forbidden = ("create", "update", "delete", "set_", "status_change", "manage")
        candidates = list(root.glob("**/commands/*/command.py")) + list(root.glob("**/events/*/event.py"))
        self.assertGreater(len(candidates), 0)
        violations = [path for path in candidates if any(word in str(path).lower() for word in forbidden)]
        self.assertEqual(violations, [], f"CRUD-shaped domain title(s): {violations}")

    def test_every_command_and_event_has_its_own_intention_directory(self) -> None:
        root = Path(__file__).parents[2]
        candidates = list(root.glob("**/commands/*/command.py")) + list(root.glob("**/events/*/event.py"))
        self.assertTrue(all(path.parent.name not in {"commands", "events"} for path in candidates))

    def test_python_type_titles_are_also_free_of_crud_language(self) -> None:
        root = Path(__file__).parents[2]
        forbidden = ("create", "update", "delete", "setstatus", "statuschange", "manage")
        candidates = list(root.glob("**/commands/*/command.py")) + list(root.glob("**/events/*/event.py"))
        titles = [
            node.name for path in candidates for node in ast.parse(path.read_text()).body
            if isinstance(node, ast.ClassDef)
        ]
        violations = [title for title in titles if any(word in title.lower() for word in forbidden)]
        self.assertEqual(violations, [], f"CRUD-shaped command/event type title(s): {violations}")


if __name__ == "__main__":
    unittest.main()
