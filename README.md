# Yunabuju (여나부주)

한국어 사용자를 위한 ActivityPub 서버 디렉토리입니다. 페디버스(Fediverse) 내의 한국어 사용 인스턴스들을 자동으로 발견하고 관리합니다.

## 필요 사항

- Node.js 16 이상
- PostgreSQL 12 이상
- npm 또는 yarn

## 설치 방법

1. PostgreSQL 데이터베이스 생성:
```bash
createdb activitypub_directory
```

2. 의존성 설치:
```bash
npm install
```

3. 환경 변수 설정:
```bash
cp .env.example .env
# .env 파일을 적절히 수정하세요
```

4. 데이터베이스 테이블 생성:
```bash
npm run setup-db
```

5. 개발 서버 실행:
```bash
npm run dev
```

## 환경 변수 설정

`.env` 파일에 다음 변수들을 설정하세요:

```
DATABASE_URL=postgres://username:password@localhost:5432/activitypub_directory
PORT=3500
NODE_ENV=development
```

## API 엔드포인트

- `GET /` - API 정보 및 상태
- `GET /yunabuju/servers` - 가입 가능한 한국어 서버 목록 조회
- `GET /yunabuju/servers/all` - 모든 한국어 서버 목록 조회 (가입 불가능한 서버 포함)
- `POST /yunabuju/discover` - 수동으로 서버 검색 시작
- `GET /yunabuju/status` - 서버 상태 및 통계 정보 조회

### 쿼리 파라미터

- `/yunabuju/servers` 엔드포인트에서 `includeClosedRegistration=true` 파라미터를 사용하면 가입이 닫힌 서버도 포함하여 조회할 수 있습니다.

## 자동 검색 스케줄

- 매일 새벽 3시(한국 시간)에 자동으로 새로운 서버를 검색합니다.
- 검색된 서버는 한국어 사용률이 30% 이상일 경우 디렉토리에 추가됩니다.

## 개발 참고사항

- 서버는 기본적으로 http://localhost:3500 에서 실행됩니다.
- 개발 모드에서는 nodemon을 통해 파일 변경 시 자동으로 서버가 재시작됩니다.
- 로그는 `error.log`와 `combined.log` 파일에 저장되며, 개발 모드에서는 콘솔에도 출력됩니다.

## 문제해결

- 데이터베이스 연결 오류 시 DATABASE_URL 환경변수를 확인해주세요.
- 포트 충돌 시 .env 파일에서 PORT를 변경해주세요.
- 로그 파일을 확인하여 자세한 오류 내용을 파악할 수 있습니다.

## 시드 서버

다음 서버들이 초기 검색을 위한 시드 서버로 설정되어 있습니다:
- mustard.blog
