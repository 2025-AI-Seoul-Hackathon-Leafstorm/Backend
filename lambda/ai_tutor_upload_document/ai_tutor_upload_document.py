import json
import os
import boto3
import datetime
from botocore.exceptions import ClientError
import base64

# 환경 변수 가져오기
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'source버킷')  # 처리 대기 버킷
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'target버킷')  # 대상 버킷
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# S3 클라이언트 초기화
s3_client = boto3.client('s3', region_name=AWS_REGION)

def check_folder_exists(folder_name):
    """
    대상 버킷에서 지정된 폴더가 존재하는지 확인
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=TARGET_BUCKET,
            Prefix=f"{folder_name}/",
            MaxKeys=1
        )
        return 'Contents' in response
    except ClientError as e:
        print(f"폴더 확인 중 오류 발생: {str(e)}")
        return False

def create_document_structure(folder_name, document_name):
    """
    대상 버킷에 문서 폴더 구조 생성:
    /{folder_name}/{document_name}/
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
        
        # 각 폴더 생성
        for folder_path in folders:
            s3_client.put_object(
                Bucket=TARGET_BUCKET,
                Key=folder_path,
                Body=''
            )
            print(f"폴더 생성 완료: s3://{TARGET_BUCKET}/{folder_path}")
        
        return True
    except Exception as e:
        print(f"문서 폴더 구조 생성 중 오류: {str(e)}")
        return False

def lambda_handler(event, context):
    """
    파일을 소스 버킷의 upload 폴더에 업로드하고, 대상 버킷에 문서 폴더 구조를 생성합니다.
    
    요청 형식:
    {
        "folder_name": "폴더명",
        "file_content": "base64로 인코딩된 파일 내용",
        "filename": "원본 파일명.pdf"
    }
    """
    try:
        # 요청에서 데이터 추출
        body = json.loads(event.get('body', '{}'))
        folder_name = body.get('folder_name')
        file_content = body.get('file_content')
        original_filename = body.get('filename')
        
        # 요청에서 file_content 추출 후 디코딩
        encoded_file_content = body.get('file_content')
        # 만약 file_content가 base64 인코딩된 문자열이라면 디코딩
        file_content = base64.b64decode(encoded_file_content)

        # 필수 파라미터 검증
        if not all([folder_name, file_content, original_filename]):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': '폴더명, 파일 내용, 파일명이 모두 필요합니다.'
                })
            }
        
        # 대상 버킷에서 폴더 존재 여부 확인
        if not check_folder_exists(folder_name):
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': f'폴더 "{folder_name}"를 찾을 수 없습니다.'
                })
            }
        
        # 문서명 추출 (확장자 제외)
        document_name = os.path.splitext(original_filename)[0]
        
        # 대상 버킷에 문서 폴더 구조 생성
        create_document_structure(folder_name, document_name)
        
        # 소스 버킷 업로드용 파일명: {folder_name}___{document_name}___{original_filename}
        upload_filename = f"{folder_name}___{document_name}___{original_filename}"
        object_key = f'upload/{upload_filename}'
        
        try:
            # 소스 버킷에 파일 업로드
            s3_client.put_object(
                Bucket=SOURCE_BUCKET,
                Key=object_key,
                Body=file_content,
                ContentType='application/pdf'  # 현재는 PDF만 지원
            )
            
            print(f"파일 업로드 성공: s3://{SOURCE_BUCKET}/{object_key}")
            
            # 생성 시간
            created_at = datetime.datetime.now().isoformat()
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': '파일 업로드 및 문서 구조 생성 성공',
                    'folder_name': folder_name,
                    'document_name': document_name,
                    'filename': original_filename,
                    'upload_path': object_key,
                    'created_at': created_at
                })
            }
            
        except ClientError as e:
            error_message = f"파일 업로드 중 오류 발생: {str(e)}"
            print(error_message)
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': error_message
                })
            }
            
    except Exception as e:
        error_message = f"Lambda 함수 실행 중 오류 발생: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': error_message
            })
        }
