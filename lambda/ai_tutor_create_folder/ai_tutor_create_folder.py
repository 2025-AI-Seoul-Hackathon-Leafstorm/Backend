import json
import os
import boto3
import uuid
import datetime
import re

# 환경 변수 가져오기
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'ai-tutor-target-docs')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# S3 클라이언트 초기화
s3 = boto3.client('s3', region_name=AWS_REGION)

def create_folder_structure(folder_name):
    """
    S3 버킷에 주제별 폴더만 생성
    """
    # S3에는 실제 폴더가 없으므로 빈 객체를 생성하여 폴더처럼 표현
    base_prefix = f"{folder_name}/"
    
    # 주제별 폴더만 생성
    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=base_prefix,
        Body=''
    )
    
    return True

def validate_folder_name(folder_name):
    """
    폴더 이름 유효성 검사
    - 영문, 숫자, 한글, 언더스코어, 하이픈만 허용
    - 공백 허용
    - 최소 2자, 최대 50자
    """
    # 특수문자 등 제한
    pattern = r'^[a-zA-Z0-9가-힣_\-\s]{2,50}$'
    
    if not re.match(pattern, folder_name):
        return False
    
    # 공백만으로 이루어진 경우 체크
    if folder_name.strip() == '':
        return False
    
    return True

def check_folder_exists(folder_name):
    """
    동일한 이름의 폴더가 이미 존재하는지 확인
    """
    response = s3.list_objects_v2(
        Bucket=TARGET_BUCKET,
        Prefix=f"{folder_name}/",
        MaxKeys=1
    )
    
    return 'Contents' in response

def lambda_handler(event, context):
    """
    새 폴더 생성 핸들러
    
    - 요청 본문에서 폴더 이름과 설명 추출
    - 폴더 이름 유효성 검사
    - S3에 폴더 구조 생성
    
    요청 형식:
    {
        "name": "폴더명",
        "description": "폴더 설명(선택)"
    }
    """
    try:
        # 요청 본문 파싱
        body = json.loads(event['body']) if 'body' in event else {}
        
        # 폴더 이름 가져오기
        folder_name = body.get('name', '').strip()
        description = body.get('description', '')
        
        # 폴더 이름 유효성 검사
        if not validate_folder_name(folder_name):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': '유효하지 않은 폴더 이름입니다. 2-50자의 영문, 숫자, 한글, 언더스코어, 하이픈만 허용됩니다.'
                })
            }
        
        # 동일 이름 폴더 존재 확인
        if check_folder_exists(folder_name):
            return {
                'statusCode': 409,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'폴더 "{folder_name}"이(가) 이미 존재합니다.'
                })
            }
        
        # 폴더 생성
        create_folder_structure(folder_name)
        
        # 성공 응답
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'name': folder_name,
                'description': description,
                'createdAt': datetime.datetime.now().isoformat(),
                'documentCount': 0
            })
        }
    
    except Exception as e:
        # 오류 응답
        print(f"Error creating folder: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'폴더 생성 중 오류가 발생했습니다: {str(e)}'
            })
        }
