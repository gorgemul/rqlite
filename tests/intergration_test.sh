PROG="rqlite"
DB="test.db"
PROMPT="rqlite>"
NEW_LINE="
"
SUCCESS_TEST_COUNT=0
FAIL_TEST_COUNT=0

PAGE_SIZE=4096
PAGE_MAX_NUMS=64;
ID_SIZE=8
NAME_MAX_SIZE=32
DESCRIPTION_MAX_SIZE=256
ROW_SIZE=$((ID_SIZE + NAME_MAX_SIZE + DESCRIPTION_MAX_SIZE))
NODE_KIND_SIZE=1
NODE_IS_ROOT_SIZE=1
NODE_PARENT_SIZE=4
NODE_N_CELLS_SIZE=4
NODE_HEADER_SIZE=$((NODE_KIND_SIZE + NODE_IS_ROOT_SIZE + NODE_PARENT_SIZE + NODE_N_CELLS_SIZE))
LEAF_NODE_NEXT_CELL_SIZE=4
LEAF_NODE_HEADER_SIZE=$((NODE_HEADER_SIZE + LEAF_NODE_NEXT_CELL_SIZE))
LEAF_NODE_CELL_SIZE=$((ROW_SIZE + ID_SIZE))
LEAF_NODE_SPACE_FOR_CELLS=$((PAGE_SIZE - LEAF_NODE_HEADER_SIZE))
LEAF_NODE_CELL_MAX_NUM=$((LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE))
SPLIT_RIGHT_LEAF_NODE_NUM=$(((LEAF_NODE_CELL_MAX_NUM + 1) / 2))
SPLIT_LEFT_LEAF_NODE_NUM=$(((LEAF_NODE_CELL_MAX_NUM + 1) - SPLIT_RIGHT_LEAF_NODE_NUM))

function setup() {
  cargo build || { echo "ERROR: build fail"; exit 1; }
  rm "$DB" > /dev/null 2>&1
  cp "../target/debug/$PROG" .
}

function teardown() {
  rm "$PROG" 
  rm -r ../target/debug
}

function exec_command() {
  local commands=("$@")
  local output=$(printf "%s\n" "${commands[@]}" | "./$PROG" "$DB" 2>&1)
  echo "$output"
}

function summary_test() {
  TEST_STATUS=$([[ "$FAIL_TEST_COUNT" -eq 0 ]] && echo "success" || echo "fail")
  echo "TEST RESULT: $TEST_STATUS. $SUCCESS_TEST_COUNT passed. $FAIL_TEST_COUNT failed."
}

# since state persist even after exist, need to make a clean db for test
function assert_and_drop_db() {
  # assert
  local got="$1"
  local expected="$2"
  local test_name="$3"
  if [[ "$got" == "$expected" ]]; then
    SUCCESS_TEST_COUNT=$(($SUCCESS_TEST_COUNT + 1))
    echo "SUCCESS: $test_name test pass"
  else
    FAIL_TEST_COUNT=$(($FAIL_TEST_COUNT + 1))
    echo "ERROR: $test_name test fail"
    echo "=========== *expected ==========="
    echo "$expected"
    echo "=========== expected* ==========="
    echo "===========   *got    ==========="
    echo "$got"
    echo "===========   got*    ==========="
  fi
  # drop db
  rm "$DB" > /dev/null 2>&1
}

function test_insert_one() {
  local commands=(
    "insert 1 foo bar"
    "select"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT executed.
$PROMPT [1, foo, bar]
executed.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "insert_one"
}

function test_insert_less_args() {
  local commands=(
    "insert foo bar"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT ERROR: insert <id> <name> <description>.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "insert_less_args"
}

function test_insert_not_num_id() {
  local commands=(
    "insert foo bar baz"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT ERROR: insert <id> <name> <description>.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "insert_not_num_id"
}

function test_insert_duplicated_id() {
  local commands=(
    "insert 1 foo1 bar1"
    "insert 2 foo2 bar2"
    "insert 1 foo bar"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT executed.
$PROMPT executed.
$PROMPT ERROR: key '1' already exist.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "insert_duplicated_id"
}

function test_insert_pass_max() {
  local commands=()
  for i in $(seq 1 $((2*SPLIT_LEFT_LEAF_NODE_NUM + SPLIT_RIGHT_LEAF_NODE_NUM))); do
    commands+=("insert $i name$i description$i")
  done
  commands+=(".exit")
  local result=$(exec_command "${commands[@]}")
  # hack for split string into array by new line
  local save_IFS=$IFS
  IFS=$'\n'
  local result_arr=($result)
  IFS="$save_IFS"
  got="${result_arr[${#result_arr[@]}-2]}" # get second to the last item
  expected="TODO: update parent after split"
  assert_and_drop_db "$got" "$expected" "insert_pass_max"
}

function test_insert_out_of_order() {
  local commands=(
    "insert 100 foo100 bar100"
    "insert 50 foo50 bar50"
    "insert 75 foo75 bar75"
    "insert 2 foo2 bar2"
    "insert 120 foo120 bar120"
    "select"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT executed.
$PROMPT executed.
$PROMPT executed.
$PROMPT executed.
$PROMPT executed.
$PROMPT [2, foo2, bar2]
[50, foo50, bar50]
[75, foo75, bar75]
[100, foo100, bar100]
[120, foo120, bar120]
executed.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "insert_out_of_order"
}

function test_negative_id() {
  local commands=(
    "insert -1 foo bar"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT ERROR: id must be greater than 0.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "negative_id"
}

function test_name_and_description_max_len() {
  local name=""
  local description=""
  for _ in $(seq 1 $NAME_MAX_SIZE); do
    name+="n"
  done
  for _ in $(seq 1 $DESCRIPTION_MAX_SIZE); do
    description+="d"
  done
  local commands=(
    "insert 1 $name $description"
    "select"
    ".exit"
)
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT executed.
$PROMPT [1, $name, $description]
executed.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "name_and_description_max_len"
}

function test_name_len_pass_max() {
  local name=""
  for _ in $(seq 1 $(($NAME_MAX_SIZE + 1))); do
    name+="n"
  done
  local commands=(
    "insert 1 $name dummpyDescription"
    ".exit"
)
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT ERROR: name too long.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "name_len_pass_max"
}

function test_description_pass_max() {
  local description=""
  for _ in $(seq 1 $((DESCRIPTION_MAX_SIZE + 1)));do
    description+="d"
  done
  local commands=(
    "insert 1 dummyName $description"
    ".exit"
)
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT ERROR: description too long.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "description_pass_max"
}

function test_persistence() {
  local commands1=(
    "insert 1 foo bar"
    ".exit"
  )
  local commands2=(
    "select"
    ".exit"
  )
  exec_command "${commands1[@]}" > /dev/null # for side effect
  local got=$(exec_command "${commands2[@]}")
  local expected="$PROMPT [1, foo, bar]
executed.
$PROMPT "
  assert_and_drop_db "$got" "$expected" "persistence"
}

function test_print_constants() {
  local commands=(
    ".constants"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="$PROMPT CONSTANT:
row size: $ROW_SIZE
node header size: $NODE_HEADER_SIZE
leaf node header size: $LEAF_NODE_HEADER_SIZE
leaf node cell size: $LEAF_NODE_CELL_SIZE
leaf node space for cells: $LEAF_NODE_SPACE_FOR_CELLS
leaf node max cells: $LEAF_NODE_CELL_MAX_NUM
$PROMPT "
  assert_and_drop_db "$got" "$expected" "print_constants"
}

function test_print_tree() {
  local commands=()
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 1))); do
    commands+=("insert $i name$i description$i")
  done
  commands+=(".tree")
  commands+=(".exit")
  local got=$(exec_command "${commands[@]}")
  local expected=""
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 1))); do
    expected+="$PROMPT executed.$NEW_LINE"
  done
  expected+="$PROMPT TREE:
- internal (size 1)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 7)
    - 8
    - 9
    - 10
    - 11
    - 12
    - 13
    - 14
$PROMPT "
  assert_and_drop_db "$got" "$expected" "print_tree"
}

function test_search_in_internal_node() {
  local commands=()
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 2))); do
    commands+=("insert $i name$i description$i")
  done
  commands+=(".exit")
  local got=$(exec_command "${commands[@]}")
  local expected=""
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 2))); do
    # can't do expected+="$PROMPT executed.\n", since bash not interpret \n correctly
    expected+="$PROMPT executed.$NEW_LINE"
  done
  expected+="$PROMPT "
  assert_and_drop_db "$got" "$expected" "search_in_internal_node"
}

function test_select_all_nodes() {
  local commands=()
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 1))); do
    commands+=("insert $i name$i description$i")
  done
  commands+=("select")
  commands+=(".exit")
  local got=$(exec_command "${commands[@]}")
  local expected=""
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 1))); do
    expected+="$PROMPT executed.$NEW_LINE"
  done
  expected+="$PROMPT "
  for i in $(seq 1 $((LEAF_NODE_CELL_MAX_NUM + 1))); do
    expected+="[$i, name$i, description$i]$NEW_LINE"
  done
  expected+="executed.$NEW_LINE"
  expected+="$PROMPT "
  assert_and_drop_db "$got" "$expected" "select_all_nodes"
}

setup
test_insert_less_args
test_insert_not_num_id
test_insert_duplicated_id
test_insert_one
test_insert_pass_max
test_insert_out_of_order
test_negative_id
test_name_and_description_max_len
test_name_len_pass_max
test_description_pass_max
test_persistence
test_print_constants
test_print_tree
test_search_in_internal_node
test_select_all_nodes
summary_test
teardown
