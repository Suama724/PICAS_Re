import sys

from tests import (
    test_01_datalayer_structure_and_timer
)

TESTS = {
    "T01": test_01_datalayer_structure_and_timer
}

if __name__ == "__main__":
    # 用法： python test_runner.py t01
    selected = sys.argv[1:]
    if selected == []:
        raise ValueError("should input the example")
    
    for name in selected:
        test_fn = TESTS[name.upper()]
        print(f"============[Test Running {name.upper()}]============")
        test_fn()
        print("==========================================")
        print("\n")