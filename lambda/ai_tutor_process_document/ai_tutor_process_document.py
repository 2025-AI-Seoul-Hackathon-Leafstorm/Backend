import json
import os
import boto3
import requests
import urllib.parse
import datetime
from botocore.exceptions import ClientError

# 환경 변수 가져오기
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'ai-tutor-source-docs')  # 처리 대기 버킷
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'ai-tutor-target-docs')  # 대상 버킷
UPSTAGE_API_ENDPOINT = os.environ.get('UPSTAGE_API_ENDPOINT', 'https://api.upstage.ai/v1/document-digitization')
UPSTAGE_API_KEY = os.environ.get('UPSTAGE_API_KEY', 'up_ZHV5KSiPKtoVUgTlQfuHiIk7LaUmg')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# S3 클라이언트 초기화
s3_client = boto3.client('s3', region_name=AWS_REGION)

def transform_result(api_result, folder_name, document_name, original_filename):
    """
    Upstage API 응답을 변환하고 메타데이터를 추가합니다.
    """
    pages_dict = {}
    for element in api_result.get("elements", []):
        page_num = element.get("page", 1)
        category = element.get("category", "unknown")
        markdown = element.get("content", {}).get("markdown", "")
        
        if page_num not in pages_dict:
            pages_dict[page_num] = []
        pages_dict[page_num].append({
            "category": category,
            "markdown": markdown
        })
    
    pages = [{"page": k, "contents": v} for k, v in sorted(pages_dict.items())]
    
    created_at = datetime.datetime.utcnow().isoformat()
    final_structure = {
        "folder_name": folder_name,
        "document_name": document_name,
        "original_filename": original_filename,
        "created_at": created_at,
        "metadata": {
            "api_version": api_result.get("api"),
            "model": api_result.get("model"),
            "total_pages": api_result.get("usage", {}).get("pages"),
            "file_type": "application/pdf",
            "indexed": False,
            "last_updated": created_at
        },
        "pages": pages
    }
    
    return final_structure

def ensure_document_structure(bucket_name, folder_name, document_name):
    """
    대상 버킷에서 문서별 폴더 구조가 존재하는지 확인하고, 필요시 생성합니다.
    /{folder_name}/{document_name}/upload/
    /{folder_name}/{document_name}/processed/
    /{folder_name}/{document_name}/chat/
    """
    try:
        # 폴더 경로 목록
        folders = [
            f"{folder_name}/{document_name}/",
            f"{folder_name}/{document_name}/upload/",
            f"{folder_name}/{document_name}/processed/",
            f"{folder_name}/{document_name}/chat/"
        ]
        
        for folder_path in folders:
            try:
                # 폴더 존재 여부 확인
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=folder_path,
                    MaxKeys=1
                )
                
                # 폴더가 없으면 생성
                if 'Contents' not in response:
                    print(f"폴더 '{folder_path}' 생성")
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=folder_path
                    )
                else:
                    print(f"폴더 '{folder_path}' 이미 존재함")
            except Exception as e:
                print(f"폴더 '{folder_path}' 확인/생성 중 오류: {str(e)}")
                # 계속 진행
        
        return True
    except Exception as e:
        print(f"폴더 구조 확인/생성 중 오류: {str(e)}")
        return False

def lambda_handler(event, context):
    """
    S3에 새 파일 업로드 시 트리거되어 처리합니다.
    파일명 형식: {folder_name}___{document_name}___{filename}.pdf
    """
    print("ai_tutor_process_document 함수 시작")

    try:
        # 1. S3 Event 처리
        records = event.get("Records", [])
        if not records:
            print("Error: 이벤트에 레코드가 없습니다.")
            return {"statusCode": 400, "body": json.dumps({"message": "No records in event"})}
        
        bucket_name = records[0]["s3"]["bucket"]["name"]
        # 소스 버킷에서만 처리
        if bucket_name != SOURCE_BUCKET:
            print(f"소스 버킷({SOURCE_BUCKET})이 아닌 {bucket_name}에서 이벤트 발생. 무시합니다.")
            return {"statusCode": 200, "body": json.dumps({"message": "Not from source bucket"})}
            
        object_key = records[0]["s3"]["object"]["key"]
        
        # Object key 디코딩 (한글 처리)
        decoded_key = urllib.parse.unquote_plus(object_key)
        print(f"처리할 객체: {bucket_name}/{decoded_key}")
        
        # upload 폴더에 있는 파일만 처리
        if not decoded_key.startswith('upload/'):
            print(f"upload 폴더의 파일이 아님: {decoded_key}")
            return {"statusCode": 200, "body": json.dumps({"message": "Not a file in upload folder"})}
        
        # 경로에서 파일명 추출
        filename = os.path.basename(decoded_key)
        
        # 지정된 폴더와 문서명 추출 (파일명에서 정보 추출)
        parts = filename.split('___', 2)
        if len(parts) != 3:
            error_message = f"잘못된 파일명 형식: '{filename}' (folder___document___filename.pdf 형식이어야 함)"
            print(error_message)
            return {"statusCode": 400, "body": json.dumps({"message": error_message})}
        
        folder_name, document_name, actual_filename = parts
        
        print(f"처리 시작: 소스 버킷: {bucket_name}, 대상 버킷: {TARGET_BUCKET}, " +
              f"폴더: {folder_name}, 문서: {document_name}, 파일: {actual_filename}")
        
        # 2. 대상 버킷에 문서별 폴더 구조 확인/생성
        ensure_document_structure(TARGET_BUCKET, folder_name, document_name)
        
        # 3. 원본 파일 다운로드
        download_path = f"/tmp/{actual_filename}"
        try:
            print(f"다운로드 시도: s3://{bucket_name}/{decoded_key}")
            s3_client.download_file(bucket_name, decoded_key, download_path)
            print(f"파일 다운로드 성공: {download_path}")
        except ClientError as e:
            error_message = f"S3 객체 다운로드 오류: {str(e)}"
            print(error_message)
            return {"statusCode": 500, "body": json.dumps({"message": error_message})}
        
        # 4. Upstage API 호출
        headers = {"Authorization": f"Bearer {UPSTAGE_API_KEY}"}
        data = {
            "ocr": "auto",
            "output_formats": "['markdown']",
            "model": "document-parse",
            "coordinates": "false"
        }
        files = {"document": open(download_path, "rb")}
        
        print("Upstage API 호출 시작")
        
        try:
            response = requests.post(UPSTAGE_API_ENDPOINT, headers=headers, files=files, data=data)
            if response.status_code != 200:
                error_message = f"Upstage API 오류: 상태 코드 {response.status_code}, 응답: {response.text}"
                print(error_message)
                return {"statusCode": 500, "body": json.dumps({"message": error_message})}
            
            result = response.json()
            print("Upstage API 응답 성공")
        except Exception as e:
            error_message = f"Upstage API 호출 오류: {str(e)}"
            print(error_message)
            return {"statusCode": 500, "body": json.dumps({"message": error_message})}
        finally:
            files["document"].close()
            print("업로드 파일 핸들 닫기 완료")
        
        # 5. 결과 변환 및 S3에 저장
        transformed_result = transform_result(result, folder_name, document_name, actual_filename)
        final_json = json.dumps(transformed_result, ensure_ascii=False, indent=2)
        
        try:
            # 1. 원본 파일을 대상 버킷의 문서별 upload/ 경로로 이동
            target_upload_key = f"{folder_name}/{document_name}/upload/{actual_filename}"
            print(f"원본 파일 복사: {SOURCE_BUCKET}/{decoded_key} -> {TARGET_BUCKET}/{target_upload_key}")
            
            s3_client.copy_object(
                Bucket=TARGET_BUCKET,
                CopySource={'Bucket': SOURCE_BUCKET, 'Key': decoded_key},
                Key=target_upload_key
            )
            
            # 2. 소스 버킷에서 원본 파일 삭제
            s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=decoded_key)
            print(f"소스 버킷에서 원본 파일 삭제 완료: {SOURCE_BUCKET}/{decoded_key}")
            
            # 3. 처리 결과를 대상 버킷의 문서별 processed/ 폴더에 저장
            result_filename = f"{document_name}_result.json"
            target_processed_key = f"{folder_name}/{document_name}/processed/{result_filename}"
            
            s3_client.put_object(
                Bucket=TARGET_BUCKET,
                Key=target_processed_key,
                Body=final_json,
                ContentType="application/json"
            )
            print(f"처리 결과 저장 완료: s3://{TARGET_BUCKET}/{target_processed_key}")
            
            # 4. 소스 버킷의 processed/ 폴더에 복사본 저장 (인덱싱 용도)
            source_processed_key = f"processed/{folder_name}_{document_name}_result.json"
            s3_client.put_object(
                Bucket=SOURCE_BUCKET, 
                Key=source_processed_key,
                Body=final_json,
                ContentType="application/json"
            )
            print(f"인덱싱용 결과 저장 완료: s3://{SOURCE_BUCKET}/{source_processed_key}")
            
        except ClientError as e:
            error_message = f"결과 저장 중 오류 발생: {str(e)}"
            print(error_message)
            return {"statusCode": 500, "body": json.dumps({"message": error_message})}
        
        # 6. 성공 응답 반환
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "문서 처리 완료",
                "folder_name": folder_name,
                "document_name": document_name,
                "original_filename": actual_filename,
                "source_bucket": SOURCE_BUCKET,
                "target_bucket": TARGET_BUCKET,
                "result_path": target_processed_key
            })
        }
        
    except Exception as e:
        error_message = f"Lambda 함수 실행 중 오류 발생: {str(e)}"
        print(error_message)
        return {"statusCode": 500, "body": json.dumps({"message": error_message})}
