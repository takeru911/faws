#!/bin/bash

set -euo pipefail

CUR_DIR=$(cd $(dirname $0); pwd)

SQS_ENDPOINT=localhost:5000
QUEUE_NAME=test-queue

# server立ち上げる
echo "LAUNCh SQS SERVER"
make -C ${CUR_DIR}/.. run/sqs > /dev/null &
BACKGROUND_PID=$!
P_GID=$(ps fj -p ${BACKGROUND_PID} | grep ${BACKGROUND_PID} | cut -d " " -f 3)
echo ${P_GID}

echo "CREATE QUEUE TEST"

ACTUAL=$(aws sqs create-queue --queue-name ${QUEUE_NAME} --endpoint http://${SQS_ENDPOINT} | jq -r '.[]')
EXPECTED="https://${SQS_ENDPOINT}/queues/${QUEUE_NAME}"

if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "failed create queue"
    echo "ACTUAL: ${ACTUAL}"
    echo "EXPECTED: ${EXPECTED}"
    kill -- -"${BACKGROUND_PID}"
    exit 1
fi


echo "LIST QUEUES TEST"

ACTUAL=$(aws sqs list-queues --endpoint http://${SQS_ENDPOINT} | jq -r '.[][0]')
EXPECTED="https://${SQS_ENDPOINT}/queues/${QUEUE_NAME}"

if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "failed list queue"
    echo "ACTUAL: ${ACTUAL}"
    echo "EXPECTED: ${EXPECTED}"
    kill -- -"${P_GID}"
    exit 1
fi

echo "SEND MESSAGE TEST"

# md5周りの実装がまだ定数なのでエラーにならないかだけかくにん
aws sqs send-message --queue-url http://${SQS_ENDPOINT}/queues/${QUEUE_NAME} --message-body "test" --endpoint http://${SQS_ENDPOINT}


echo "RECEIVE MESSAGE TEST"

# md5周りの実装がまだ定数なのでエラーにならないかだけかくにん
ACTUAL=$(aws sqs receive-message --queue-url http://${SQS_ENDPOINT}/queues/${QUEUE_NAME} --endpoint http://${SQS_ENDPOINT} | jq -r '.Messages[0].Body')
EXPECTED="test"

if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "failed receive queue"
    echo "ACTUAL: ${ACTUAL}"
    echo "EXPECTED: ${EXPECTED}"
    kill -- -"${P_GID}"
    exit 1
fi



# 配信遅延が正常に動くか
aws sqs create-queue --queue-name delay-queue --endpoint http://${SQS_ENDPOINT}
aws sqs send-message --queue-url http://${SQS_ENDPOINT}/queues/delay-queue --message-body "test" --endpoint http://${SQS_ENDPOINT} --delay-seconds 5

# 遅延が5秒なのでメッセージは受信できるものはない
ACTUAL=$(aws sqs receive-message --queue-url http://${SQS_ENDPOINT}/queues/delay-queue --endpoint http://${SQS_ENDPOINT})
EXPECTED=""
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "failed set delay message"
    echo "ACTUAL: ${ACTUAL}"
    echo "EXPECTED: ${EXPECTED}"
    kill -- -"${P_GID}"
    exit 1
fi

# 8秒待機し、遅延が解除され受信できる
sleep 8
ACTUAL=$(aws sqs receive-message --queue-url http://${SQS_ENDPOINT}/queues/delay-queue --endpoint http://${SQS_ENDPOINT} |  jq -r '.Messages[0].Body')
EXPECTED="test"

if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "failed set delay message"
    echo "ACTUAL: ${ACTUAL}"
    echo "EXPECTED: ${EXPECTED}"
    kill -- -"${P_GID}"
    exit 1
fi


echo "TERMINATE SQS SERVER"
set +e
kill -- -"${P_GID}"
set -e

exit 0
