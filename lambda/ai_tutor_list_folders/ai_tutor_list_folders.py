import json
import os
import boto3
from collections import defaultdict
from botocore.exceptions import ClientError

# 환경 변수 가져오기
DOCS_BUCKET = os.environ['DOCS_BUCKET']
AWS_REGION = os.environ['AWS_REGION']

# S3 클라이언트 초기화
s3 = boto3.client('s3', region_name=AWS_REGION)

# 필요한 권한: s3:ListBucket (List 작업에 필요)

def get_document_count(folder_name):
    """
    특정 폴더의 processed/ 디렉토리에 있는 문서 수 조회
    
    필요한 IAM 권한:
    - s3:ListBucket
    """
    try:
        response = s3.list_objects_v2(
            Bucket=DOCS_BUCKET,
            Prefix=f"{folder_name}/processed/",
            Delimiter="/"
        )
    except ClientError as e:
        print(f"문서 수 조회 오류: {str(e)}")
        return 0
    
    # 'Contents' 키가 있으면 파일이 존재
    if 'Contents' in response:
        # 첫 번째 항목은 디렉토리 자체이므로 제외 (-1)
        return len(response['Contents']) - 1 if len(response['Contents']) > 0 else 0
    else:
        return 0

def lambda_handler(event, context):
    """
    S3에서 모든 폴더(주제) 목록 조회
    
    - S3의 최상위 디렉토리 목록 조회
    - 각 폴더의 문서 수 계산
    
    필요한 IAM 권한:
    - s3:ListBucket
    """
    try:
        # S3에서 최상위 '폴더' 목록 조회 (구분자 '/' 사용)
        try:
            response = s3.list_objects_v2(
                Bucket=DOCS_BUCKET,
                Delimiter='/'
            )
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
        
        folders = []
        
        # CommonPrefixes에는 구분자로 끝나는 접두사(즉, 폴더)가 포함됨
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                folder_prefix = prefix['Prefix']
                # 마지막 '/' 제거하여 폴더 이름 추출
                folder_name = folder_prefix.rstrip('/')
                
                # 문서 수 조회
                document_count = get_document_count(folder_name)
                
                # 폴더 정보 생성
                folder_info = {
                    'name': folder_name,
                    'documentCount': document_count
                }
                
                # 추가 메타데이터 조회 시도 (향후 확장성 고려)
                try:
                    meta_response = s3.list_objects_v2(
                        Bucket=DOCS_BUCKET,
                        Prefix=f"{folder_name}/metadata.json"
                    )
                    
                    if 'Contents' in meta_response and len(meta_response['Contents']) > 0:
                        metadata_obj = s3.get_object(
                            Bucket=DOCS_BUCKET, 
                            Key=f"{folder_name}/metadata.json"
                        )
                        metadata = json.loads(metadata_obj['Body'].read().decode('utf-8'))
                        
                        # 메타데이터에서 설명 추출
                        if 'description' in metadata:
                            folder_info['description'] = metadata['description']
                        if 'createdAt' in metadata:
                            folder_info['createdAt'] = metadata['createdAt']
                except Exception as e:
                    # 메타데이터 파일이 없거나 읽을 수 없는 경우 무시
                    print(f"메타데이터 조회 오류 (무시됨): {str(e)}")
                
                folders.append(folder_info)
        
        # 문서 수 기준 내림차순 정렬 (선택적)
        folders.sort(key=lambda x: x['documentCount'], reverse=True)
        
        # 성공 응답
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'folders': folders,
                'count': len(folders)
            })
        }
    
    except Exception as e:
        # 오류 응답
        print(f"Error listing folders: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'폴더 목록 조회 중 오류가 발생했습니다: {str(e)}'
            })
        }
