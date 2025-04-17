import json
import os
import boto3
import sys
from openai import OpenAI  # openai 패키지 (openai==1.52.2)

# S3 클라이언트 초기화
s3_client = boto3.client("s3")
# 결과 JSON 파일이 저장된 S3 버킷명 (환경변수 또는 기본값)
RESULT_BUCKET = os.environ.get("RESULT_BUCKET", "target버킷")


def lambda_handler(event, context):
    """
    API Gateway에서 전달받은 document_id (S3 객체 키)를 이용해 JSON 파일을 읽고,
    해당 파일의 내용을 기반으로 Upstage의 solar‑pro 모델에 요약 요청을 보내어 요약본을 반환하며,
    요약본을 Markdown 파일 형식으로 S3에 저장합니다.
    """
    # 1. API Gateway 쿼리 파라미터에서 document_id 추출
    try:
        document_id = event["queryStringParameters"]["document_id"]
    except Exception as e:
        error_message = "document_id 파라미터가 필요합니다."
        print(error_message)
        sys.stdout.flush()
        return {"statusCode": 400, "body": error_message}
    
    print(f"요청받은 document_id (S3 객체 키): {document_id}")
    sys.stdout.flush()
    
    # 2. S3에서 JSON 파일 가져오기
    try:
        s3_response = s3_client.get_object(Bucket=RESULT_BUCKET, Key=document_id)
        file_content = s3_response["Body"].read().decode("utf-8")
        document_json = json.loads(file_content)
    except Exception as e:
        error_message = f"S3에서 JSON 파일을 가져오는 중 오류 발생: {str(e)}"
        print(error_message)
        sys.stdout.flush()
        return {"statusCode": 500, "body": error_message}
    
    # 3. 대화형 프롬프트 구성
    prompt_text = (
        "You are a professional technical writer helping students prepare for exams.\n"
        "Summarize the following content with a focus on **key points likely to be tested in an exam**.\n"
        "The output must be in clean, structured, and condensed **Markdown format** suitable for Notion.\n"
        "Use clear section titles (##), bullet points (-), and tables if helpful.\n"
        "Ignore any metadata, introductions, or copyright information.\n"
        "Prioritize concepts, definitions, processes, and comparisons that are important for test-taking.\n"
        "Avoid verbosity. Be direct and focused.\n\n"
        
        "Now summarize the following lecture document with that goal in mind:\n\n"
        + json.dumps(document_json, ensure_ascii=False, indent=2)
    )
    
    # 4. Upstage의 solar‑pro 모델 호출을 위한 클라이언트 생성
    client = OpenAI(
        api_key=os.environ.get("UPSTAGE_API_KEY", "up_ZHV5KSiPKtoVUgTlQfuHiIk7LaUmg"),
        base_url="https://api.upstage.ai/v1"
    )
    
    try:
        # Chat Completion API 호출 (동기 호출, stream=False)
        response = client.chat.completions.create(
            model="solar-pro",
            messages=[{"role": "user", "content": prompt_text}],
            temperature=0.2,
            top_p=0.4,
            stream=False,
            max_tokens=4000
        )
        print(response)
        summary_result = response.choices[0].message.content
    except Exception as e:
        error_message = f"Solar‑pro 모델 호출 중 오류 발생: {str(e)}"
        print(error_message)
        sys.stdout.flush()
        return {"statusCode": 500, "body": error_message}
    
    # 5. Markdown 파일 형식으로 S3에 저장하기
    # 기존 JSON 파일 이름에서 확장자만 .md로 변경하여 저장할 수 있습니다.
    markdown_key = document_id.rsplit('.', 1)[0] + '.md'
    try:
        s3_client.put_object(
            Bucket=RESULT_BUCKET,
            Key=markdown_key,
            Body=summary_result,
            ContentType='text/markdown'  # Content-Type 지정: Notion에서 인식하기 좋습니다.
        )
        print(f"Markdown 파일이 {RESULT_BUCKET}/{markdown_key} 에 저장되었습니다.")
    except Exception as e:
        error_message = f"S3에 Markdown 파일 저장 중 오류 발생: {str(e)}"
        print(error_message)
        sys.stdout.flush()
        return {"statusCode": 500, "body": error_message}
    
    # 6. 최종 요약 결과를 API 응답으로 반환
    response_body = {
        "document_id": document_id,
        "summary": summary_result,
        "markdown_file": f"{RESULT_BUCKET}/{markdown_key}"
    }
    print("최종 요약 결과:")
    print(response_body)
    sys.stdout.flush()
    
    return {
        "statusCode": 200,
        "body": json.dumps(response_body, ensure_ascii=False)
    }
