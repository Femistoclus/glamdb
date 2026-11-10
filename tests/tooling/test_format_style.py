import importlib.util
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("quality", ROOT / "scripts/quality.py")
quality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quality)

SOURCE = """namespace NGlamDB {
struct TEmpty {};
class TStore: public IStore, public IStatistics {public:TStore(int count, bool active): Count_(count), Active_(active) {} int GetCount() const {return Count_;} private:int Count_;bool Active_;};
void Process(const TStore &store, TStore *output) {if (store.GetCount()) {output->Update();} else {return;} auto callback=[](int value){return value+1;}; switch (store.GetCount()){case 1:{First();Second();break;}default:return;}}
void LongCondition() {if (CheckVeryLongConditionWithEnoughCharactersToForceAContinuationLine() && CheckAnotherVeryLongConditionOnTheSecondLine()) {Process();}}
void LongCall() {ProcessWithLongArguments(firstArgumentWithEnoughCharactersToForceLineBreaking,secondArgumentWithEnoughCharactersToForceLineBreaking,thirdArgumentWithEnoughCharactersToForceLineBreaking);}
}
"""

EXPECTED = """namespace NGlamDB {
struct TEmpty
{ };
class TStore
    : public IStore
    , public IStatistics
{
public:
    TStore(int count, bool active)
        : Count_(count)
        , Active_(active)
    { }
    int GetCount() const
    {
        return Count_;
    }

private:
    int Count_;
    bool Active_;
};
void Process(const TStore& store, TStore* output)
{
    if (store.GetCount()) {
        output->Update();
    } else {
        return;
    }
    auto callback = [](int value) {
        return value + 1;
    };
    switch (store.GetCount()) {
        case 1: {
            First();
            Second();
            break;
        }
        default:
            return;
    }
}
void LongCondition()
{
    if (CheckVeryLongConditionWithEnoughCharactersToForceAContinuationLine() &&
        CheckAnotherVeryLongConditionOnTheSecondLine())
    {
        Process();
    }
}
void LongCall()
{
    ProcessWithLongArguments(firstArgumentWithEnoughCharactersToForceLineBreaking,
        secondArgumentWithEnoughCharactersToForceLineBreaking,
        thirdArgumentWithEnoughCharactersToForceLineBreaking);
}
}
"""


class FormatStyleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.formatter = quality.llvm_tool("clang-format")
        except RuntimeError as error:
            raise unittest.SkipTest(str(error)) from error

    def format(self, source):
        return subprocess.run(
            [self.formatter, "--style=file:" + str(ROOT / ".clang-format"),
             "--assume-filename=style_sample.cpp"],
            input=source, text=True, capture_output=True, check=True
        ).stdout

    def test_style_and_idempotence(self):
        self.assertEqual(self.format(SOURCE), EXPECTED)
        self.assertEqual(self.format(EXPECTED), EXPECTED)
        self.assertTrue(all(len(line) <= 120 for line in EXPECTED.splitlines()))

    def test_column_limit_and_required_braces(self):
        line = '    auto message = "' + "x" * 90 + '";'
        result = self.format("void Process() {\n" + line + "\nif (ready) return;\n}\n")
        self.assertIn(line, result)
        self.assertIn("    if (ready) {\n        return;\n    }", result)

    def test_preserves_include_groups_and_sorts_each_group(self):
        source = '#include "z.h"\n#include "a.h"\n\n#include <zlib.h>\n#include <arrow/api.h>\n\n#include <vector>\n#include <string>\n'
        expected = '#include "a.h"\n#include "z.h"\n\n#include <arrow/api.h>\n#include <zlib.h>\n\n#include <string>\n#include <vector>\n'
        self.assertEqual(self.format(source), expected)


if __name__ == "__main__":
    unittest.main()
