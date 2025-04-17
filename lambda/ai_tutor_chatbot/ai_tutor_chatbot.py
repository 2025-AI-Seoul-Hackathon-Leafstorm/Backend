import json
import os
import boto3
from openai import OpenAI  # pip install openai==1.52.2
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# OpenAI 클라이언트 초기화
openai_client = OpenAI(
    api_key="api-key",
    base_url=os.environ.get("OPENAI_BASE_URL", "https://api.upstage.ai/v1")
)

# DynamoDB와 S3 클라이언트 초기화
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("테이블 명칭")  # 테이블 이름 직접 입력
s3_client = boto3.client('s3')
S3_BUCKET = "버킷 명칭"  # S3 버킷 이름 직접 입력

def chat_with_solar(messages):
    response = openai_client.chat.completions.create(
        model="solar-pro",
        messages=messages
    )
    return response.choices[0].message.content

def detect_page_number(user_message):
    prompt = (
        f"다음 메시지가 문서의 특정 페이지 내용을 참조하는 질문인지 판단하고, "
        f"만약 그렇다면 페이지 번호를 'PAGE_NUMBER: <번호>' 형식으로 반환해주세요. "
        f"해당되지 않으면 'NO_PAGE'라고 답해주세요.\n\n"
        f"메시지: {user_message}"
    )
    messages = [{"role": "user", "content": prompt}]
    result = chat_with_solar(messages)
    if result.strip().upper().startswith("PAGE_NUMBER:"):
        try:
            page_num = result.split("PAGE_NUMBER:")[1].strip()
            return page_num
        except Exception as e:
            logger.error("페이지 번호 추출 오류: %s", e)
            return None
    return None

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))
        
        # queryStringParameters를 안전하게 추출
        params = event.get("queryStringParameters", {})
        logger.info("Query string parameters: %s", json.dumps(params))
        
        session_id = params.get("session_id")
        user_message = params.get("message")
        document_path = params.get("document_path")  # 선택 사항
        
        # 추출한 값을 로그로 남김
        logger.info("Extracted parameters - session_id: %s, user_message: %s, document_path: %s",
                    session_id, user_message, document_path)
        
        if not session_id or not user_message:
            error_msg = "Missing session_id or message parameter"
            logger.error(error_msg)
            return {
                "statusCode": 400,
                "body": json.dumps({"error": error_msg})
            }
        
        # DynamoDB에서 기존 대화 이력 조회
        response_db = table.get_item(Key={'tt': session_id})
        conversation = response_db.get('Item', {}).get('messages', [])
        
        # 사용자 메시지 추가
        conversation.append({"role": "user", "content": user_message})
        
        # 페이지 관련 여부 판단: 첫 번째 AI 모델 호출
        page_number = detect_page_number(user_message)
        logger.info("판단된 페이지 번호: %s", page_number)
        
        # 페이지 번호가 추출되고, document_path가 있다면 S3에서 문서 내용 로드
        if document_path and page_number:
            try:
                s3_resp = s3_client.get_object(Bucket=S3_BUCKET, Key=document_path)
                document_content = s3_resp['Body'].read().decode('utf-8')
                doc_json = json.loads(document_content)
                page_content = doc_json.get('pages', {}).get(str(page_number))
                if page_content:
                    system_message = f"Provided document page content (Page {page_number}): {page_content}"
                    conversation.append({"role": "system", "content": system_message})
            except Exception as e:
                logger.error("문서 로드 오류: %s", e)
        
        # 두 번째 호출: 전체 대화 이력을 바탕으로 최종 AI 응답 생성
        ai_response = chat_with_solar(conversation)
        conversation.append({"role": "assistant", "content": ai_response})
        
        # 업데이트된 대화 이력을 DynamoDB에 저장
        table.put_item(
            Item={
                'tt': session_id,
                'messages': conversation
            }
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "tt": session_id,
                "messages": conversation
            })
        }
    
    except Exception as e:
        logger.error("Error processing request: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
