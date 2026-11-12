<!-- FF 매뉴얼 화면에서 제목·본문·목록의 왼쪽 열을 맞추기 위한 스타일입니다. GitHub 렌더링에는 영향을 주지 않습니다. -->
<style>
#text_div ol, #text_div ul { padding-left: 0; margin-left: 0; list-style-position: inside; }
#text_div ol ol, #text_div ol ul, #text_div ul ol, #text_div ul ul { padding-left: 1.5em; }
#text_div li > p { display: inline; }
</style>

# SHYNI MATE

shyni 전용 스캔 매니저.

수신 항목을 정해진 수만큼 동시 처리한다. 항목마다 VFS 새로고침 → 스캔 전송 → 결과 확인 순서로 진행하며, VFS 새로고침은 여부를 선택할 수 있다.
동시 스캔 수는 설정으로 조절한다 (기본 1).

## 설치

1. SHYNI MATE → 설정에서 shyni 주소와 토큰을 입력하고 저장한다.
2. 스캔 → 설정에서 동시 스캔 수, VFS 새로고침 규칙, 샤이니 경로 매핑을 확인한다.

## 기능

- 모드: ADD, `REMOVE_FILE`, `REMOVE_FOLDER`, REFRESH
- 스캔 전송 전 rclone VFS 새로고침 (사용 여부 선택 가능, 규칙 기반 경로 변환)
- shyni job 폴링으로 완료/실패 확정, 완료 시 콜백 통지
- 탐색 페이지에서 우클릭으로 새로고침·스캔 실행
- 대시보드 탭: 라이브러리별 VFS 새로고침·스캔 실행과 상태 표시

## 라이선스

MIT — [LICENSE](LICENSE) 참조
