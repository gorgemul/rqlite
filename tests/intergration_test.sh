NAME_LEN_MAX=32
DESCRIPTION_LEN_MAX=256
ROW_MAX=896
SUCCESS_TEST_COUNT=0
FAIL_TEST_COUNT=0

function exec_command() {
  commands=("$@")
  output=$(printf "%s\n" "${commands[@]}" | ./rqlite)
  echo "$output"
}

function setup() {
  cargo build || "build fail"
  cp ../target/debug/rqlite .
}

function teardown() {
  rm rqlite
  rm -r ../target/debug
}

function test_insert_one() {
  local commands=(
    "insert 1 john goodstuent"
    "select"
    ".exit"
  )
  local got=$(exec_command "${commands[@]}")
  local expected="rqlite> success!
rqlite> 1. john goodstuent
success!
rqlite> "
  if [[ "$got" == "$expected" ]]; then
    SUCCESS_TEST_COUNT=$(($SUCCESS_TEST_COUNT + 1))
    echo "SUCCESS: insert_one pass"
  else
    FAIL_TEST_COUNT=$(($FAIL_TEST_COUNT + 1))
    echo "ERROR: insert_one fail"
    echo "expected: $expected"
    echo "got: $got"
  fi
}

function test_insert_pass_max() {
  local commands=()
  for i in $(seq 1 $((ROW_MAX + 1))); do
    commands+=("insert $i name$i description$i")
  done
  local result=$(exec_command "${commands[@]}")
  # hack for split string into array by new line
  local save_IFS=$IFS
  IFS=$'\n'
  local result_arr=($result)
  IFS="$save_IFS"
  got="${result_arr[${#result_arr[@]}-2]}" # get second to the last item
  expected="rqlite> table reach max size"
  if [[ "$got" == "$expected" ]]; then
    SUCCESS_TEST_COUNT=$(($SUCCESS_TEST_COUNT + 1))
    echo "SUCCESS: insert_pass_max pass"
  else
    FAIL_TEST_COUNT=$(($FAIL_TEST_COUNT + 1))
    echo "ERROR: insert_pass_max fail"
    echo "expected: $expected"
    echo "got: $got"
  fi
}

function test_name_and_description_len_max() {
  local name=""
  local description=""
  for _ in $(seq 1 $NAME_LEN_MAX); do
    name+="n"
  done
  for _ in $(seq 1 $DESCRIPTION_LEN_MAX); do
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
  if [[ "$got" == "$expected" ]]; then
    SUCCESS_TEST_COUNT=$(($SUCCESS_TEST_COUNT + 1))
    echo "SUCCESS: full_len_name_and_description pass"
  else
    FAIL_TEST_COUNT=$(($FAIL_TEST_COUNT + 1))
    echo "ERROR: full_len_name_and_description fail"
    echo "expected: $expected"
    echo "got: $got"
  fi
}

function test_name_len_pass_max() {
  local name=""
  for _ in $(seq 1 $(($NAME_LEN_MAX + 1))); do
    name+="n"
  done
  local commands=(
    "insert 1 $name dummpyDescription"
    ".exit"
)
  local got=$(exec_command "${commands[@]}")
  local expected="rqlite> name too long
rqlite> "
  if [[ "$got" == "$expected" ]]; then
    SUCCESS_TEST_COUNT=$(($SUCCESS_TEST_COUNT + 1))
    echo "SUCCESS: name_len_pass_max pass"
  else
    FAIL_TEST_COUNT=$(($FAIL_TEST_COUNT + 1))
    echo "ERROR: name_len_pass_max fail"
    echo "expected: $expected"
    echo "got: $got"
  fi
}

function test_description_pass_max() {
  local description=""
  for _ in $(seq 1 $((DESCRIPTION_LEN_MAX + 1)));do
    description+="d"
  done
  local commands=(
    "insert 1 dummyName $description"
    ".exit"
)
  local got=$(exec_command "${commands[@]}")
  local expected="rqlite> description too long
rqlite> "
  if [[ "$got" == "$expected" ]]; then
    SUCCESS_TEST_COUNT=$(($SUCCESS_TEST_COUNT + 1))
    echo "SUCCESS: description_pass_max pass"
  else
    FAIL_TEST_COUNT=$(($FAIL_TEST_COUNT + 1))
    echo "ERROR: description_pass_max fail"
    echo "expected: $expected"
    echo "got: $got"
  fi
}

setup
test_insert_one
test_insert_pass_max
test_name_and_description_len_max
test_name_len_pass_max
test_description_pass_max
TEST_STATUS=$([[ "$FAIL_TEST_COUNT" -eq 0 ]] && echo "success" || echo "fail")
echo "TEST RESULT: $TEST_STATUS. $SUCCESS_TEST_COUNT passed. $FAIL_TEST_COUNT failed."
teardown
