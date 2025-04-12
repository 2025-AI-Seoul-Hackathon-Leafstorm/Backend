import json
import os
import boto3
from botocore.exceptions import ClientError
import re

# 환경 변수 가져오기
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'ai-tutor-target-docs')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# S3 클라이언트 초기화
s3 = boto3.client('s3', region_name=AWS_REGION)

def lambda_handler(event, context):
    """
    특정 폴더의 문서 폴더 목록 조회
    
    - 경로 파라미터에서 폴더 이름 추출
    - S3에서 해당 폴더 내의 문서 폴더 목록 조회
    
    필요한 IAM 권한:
    - s3:ListBucket
    - s3:GetObject
    """
    try:
        # 1. 경로 파라미터에서 폴더 이름 추출
        folder_name = event.get('pathParameters', {}).get('id')
        
        if not folder_name:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': '폴더 이름이 제공되지 않았습니다.'
                })
            }
        
        # 2. 폴더 존재 여부 확인
        try:
            response = s3.list_objects_v2(
                Bucket=TARGET_BUCKET,
                Prefix=f"{folder_name}/",
                MaxKeys=1
            )
            
            if 'Contents' not in response:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': f'폴더 "{folder_name}"를 찾을 수 없습니다.'
                    })
                }
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print("접근 권한 오류: s3:ListBucket 권한이 필요합니다.")
                return {
                    'statusCode': 403,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'S3 버킷 접근 권한이 없습니다. 필요한 권한: s3:ListBucket'
                    })
                }
            else:
                raise
        
        # 3. 폴더 내의 모든 콘텐츠 조회
        response = s3.list_objects_v2(
            Bucket=TARGET_BUCKET,
            Prefix=f"{folder_name}/"
        )
        
        # 4. 문서 폴더 추출
        documents = []
        processed_documents = {}  # 처리 결과가 있는 문서 추적을 위한 딕셔너리
        
        if 'Contents' in response:
            # 첫 번째 패스: processed 폴더에서 메타데이터 파일 찾기
            for item in response['Contents']:
                # processed/ 경로에서 _result.json 파일 찾기
                if '/processed/' in item['Key'] and item['Key'].endswith('_result.json'):
                    try:
                        # 경로에서 문서 이름 추출 (패턴: {folder}/{document}/processed/{document}_result.json)
                        key_parts = item['Key'].split('/')
                        if len(key_parts) >= 4:
                            document_name = key_parts[1]  # 문서 폴더명
                            
                            # 메타데이터 JSON 파일 읽기
                            file_response = s3.get_object(
                                Bucket=TARGET_BUCKET,
                                Key=item['Key']
                            )
                            file_content = file_response['Body'].read().decode('utf-8')
                            document_data = json.loads(file_content)
                            
                            # 필요한 정보 추출
                            processed_documents[document_name] = {
                                'id': document_name,
                                'title': document_name,  # 파일명을 제목으로 사용
                                'createdAt': document_data.get('created_at', ''),
                                'totalPages': len(document_data.get('pages', [])),
                                'fileType': document_data.get('metadata', {}).get('file_type', 'application/pdf'),
                                'original_filename': document_data.get('original_filename', ''),
                                'isProcessed': True,
                                'processedKey': item['Key']
                            }
                    except Exception as e:
                        print(f"문서 메타데이터 처리 오류 (건너뜀): {str(e)}")
                        continue
            
            # 두 번째 패스: 모든 문서 폴더 찾기
            document_folders = set()
            for item in response['Contents']:
                # 경로 패턴: {folder}/{document}/
                if item['Key'].count('/') >= 2:
                    parts = item['Key'].split('/', 2)
                    if len(parts) >= 2:
                        document_name = parts[1]
                        if document_name:
                            document_folders.add(document_name)
            
            # 문서 폴더를 기반으로 결과 구성
            for document_name in document_folders:
                if document_name in processed_documents:
                    # 이미 처리된 메타데이터가 있는 경우
                    documents.append(processed_documents[document_name])
                else:
                    # 메타데이터가 없는 경우 기본 정보만 포함
                    documents.append({
                        'id': document_name,
                        'title': document_name,
                        'createdAt': '',  # 메타데이터가 없으므로 빈 값
                        'totalPages': 0,
                        'fileType': 'application/pdf',
                        'isProcessed': False
                    })
        
        # 5. 생성일 기준 내림차순 정렬 (최신 문서가 상위에 표시)
        # createdAt이 없는 경우 맨 뒤로 정렬
        documents.sort(key=lambda x: x.get('createdAt', '') or '0', reverse=True)
        
        # 6. 성공 응답
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'documents': documents,
                'count': len(documents),
                'folderName': folder_name
            })
        }
        
    except Exception as e:
        # 예상치 못한 오류 처리
        error_message = f"문서 목록 조회 중 오류가 발생했습니다: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': error_message
            })
        }
