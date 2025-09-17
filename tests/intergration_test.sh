PROG="rqlite"
DB="test.db"
SUCCESS_TEST_COUNT=0
FAIL_TEST_COUNT=0

PAGE_SIZE=4096
PAGE_MAX_NUMS=64;
ID_SIZE=8
NAME_MAX_SIZE=32
DESCRIPTION_MAX_SIZE=256
ROW_SIZE=$(($ID_SIZE + $NAME_MAX_SIZE + $DESCRIPTION_MAX_SIZE))
ROWS_PER_PAGE=$(($PAGE_SIZE / $ROW_SIZE))
ROW_MAX=$(($PAGE_MAX_NUMS * $ROWS_PER_PAGE))

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
  local output=$(printf "%s\n" "${commands[@]}" | "./$PROG" "$DB")
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
    echo "=========== expected ==========="
    echo "$expected"
    echo "===========   got    ==========="
    echo "$got"
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
  local expected="rqlite> success!
rqlite> 1. foo bar
success!
rqlite> "
  assert_and_drop_db "$got" "$expected" "insert_one"
}

function test_insert_pass_max() {
  local commands=()
  for i in $(seq 1 $((ROW_MAX + 1))); do
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
  expected="rqlite> table reach max size"
  assert_and_drop_db "$got" "$expected" "insert_pass_max"
}

function test_negative_id() {
  local commands=(
    "insert -1 foo bar"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="rqlite> id must be greater than 0
rqlite> "
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
  local expected="rqlite> success!
rqlite> 1. $name $description
success!
rqlite> "
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
  local expected="rqlite> name too long
rqlite> "
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
  local expected="rqlite> description too long
rqlite> "
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
  got=$(exec_command "${commands2[@]}")
  local expected="rqlite> 1. foo bar
success!
rqlite> "
  assert_and_drop_db "$got" "$expected" "persistence"
}

setup
test_insert_one
test_insert_pass_max
test_negative_id
test_name_and_description_max_len
test_name_len_pass_max
test_description_pass_max
test_persistence
summary_test
teardown
