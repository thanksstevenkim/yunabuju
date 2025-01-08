# Automated Korean ActivityPub Server Aggregator - 개발 환경 설정

## 필요 사항

- Node.js 16 이상
- PostgreSQL 12 이상
- npm 또는 yarn

## 설치 방법

1. PostgreSQL 데이터베이스 생성:
   \`\`\`bash
   createdb yunabuju
   \`\`\`

2. 의존성 설치:
   \`\`\`bash
   npm install
   \`\`\`

3. 환경 변수 설정:
   \`\`\`bash
   cp .env.example .env

# .env 파일을 적절히 수정

\`\`\`

4. 데이터베이스 테이블 생성:
   \`\`\`bash
   npm run setup-db
   \`\`\`

5. 개발 서버 실행:
   \`\`\`bash
   npm run dev
   \`\`\`

## API 엔드포인트

- GET /servers - 모든 한국어 서버 목록 조회
- GET /servers/active - 활성화된 한국어 서버만 조회

## 개발 참고사항

- 서버는 기본적으로 http://localhost:3000 에서 실행됩니다.
- 개발 모드에서는 nodemon을 통해 파일 변경 시 자동으로 서버가 재시작됩니다.
- 로그는 콘솔에 출력됩니다.

## 문제해결

- 데이터베이스 연결 오류 시 DATABASE_URL 환경변수를 확인해주세요.
- 포트 충돌 시 .env 파일에서 PORT를 변경해주세요.
