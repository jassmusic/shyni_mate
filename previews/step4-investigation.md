# Step4 사전 조사 — 관리자 API 전환 대상 정리

- 조사 기준: 로컬 소스 `~/workspace/projects/shyni_mate/` (읽기만 함, 코드 수정 없음)
- 조사일: 2026-11-17
- 전제: 관리자 세션 토큰은 `shyni_admin.py`의 `admin_get()` / `admin_request()`로 사용한다.
  현재 이 모듈의 실제 사용처는 설정 탭의 로그인 테스트(`mod_base.py:138`)뿐이다.
  Step4가 관리자 API의 첫 실전 사용이 된다.

## 참고 — 관리자 API 클라이언트 현황

| 위치 | 내용 |
|---|---|
| `shyni_admin.py:185` `admin_request(method, path)` | `Authorization: Bearer <세션 토큰>`으로 호출. 401이면 토큰을 지우고 재로그인해 1회 재시도. 기본 타임아웃 15초. 요청 자체가 실패하면 `None` 반환 |
| `shyni_admin.py:220` `admin_get(path)` | GET 전용 래퍼. 200이면 JSON dict, 아니면 `None` |

## 참고 — 조사에서 전제로 삼은 관리자 API (shyni 소스·실측 확인분)

| API | 반환 (확인된 범위) | 비고 |
|---|---|---|
| `GET /libraries/{library_id}/trash` | part_id·file_path·status·missing_since·deleted_at·deleted_reason·title·folder_path | `status <> 'available'`라 missing+trashed 둘 다. Relay의 `deleted_at IS NOT NULL`과 같은 범위 |
| `GET /library/sections` | 라이브러리 목록 (key·title·type 등) | 이미 compat 토큰으로 쓰고 있음 (`mod_status.py:245`) |
| `GET /library/metadata/{id}` | 항목 정보 | 음악 API와 함께 항목 조회에 사용 가능 |
| `GET /libraries/{id}/scan/status` | status·stage·processed/total·current_file·stage_processed/stage_total | shyni 서버 메모리 상태. 관리자 세션 필요, mate API 키로는 401 |
| `GET /libraries/{library_id}` (라이브러리 상세) | 보관 기간(`trash_retention_days`) 포함 | |

**확인 필요 사항 (구현 전 실측)**: `GET /libraries/{id}/trash` 응답에 `metadata_item_id`·`parent_id`가
있는지 미확인 — 휴지통 그룹핑 방식을 정하는 데 필요하다. 없으면 폴더 경로 기준 그룹핑으로
바꿔야 한다 (2번 항목 참조).

---

## 1. 대시보드 통계 — `task_status.py` + `db_support.py`

### 1-1. 휴지통 개수 — API 대체 가능

| 항목 | 내용 |
|---|---|
| 파일·줄 | `task_status.py:264` `trash_counts_for_stats()` → `task_status.py:274`·`:296`에서 `db_support.trash_counts()` 호출 |
| 현재 동작 | `db_support.py:484` `trash_counts()`가 `media_parts`에서 `deleted_at IS NOT NULL`인 행을 라이브러리별 `COUNT(*)`로 센다. 결과는 `status_trash_counts_cache`에 저장하고, 설정 주기(`base_trash_counts_refresh_hours`, 기본 12시간) 안이면 캐시를 재사용한다 |
| Relay에서 읽는 것 | 라이브러리별 휴지통 파일 수 `{library_id: count}` |
| API 대체 | **가능** — `GET /libraries/{library_id}/trash`의 목록 길이로 개수를 구한다. 단, 개수만 얻으려고 전체 목록을 받아야 해서 COUNT 쿼리보다 무겁다. 기존 12시간 캐시 주기가 있어 부담은 완화된다 |
| 비고 | `compute_stats()`는 이미 `trash_map` 인자로 개수를 주입받는 구조(`db_support.py:121` `library_stats(trash_map=...)`)라, 개수만 API로 바꿔도 대시보드 '휴지통' 열은 API 출처가 된다 |

### 1-2. 라이브러리별 통계 — API 대체 불가 (Relay 유지)

| 항목 | 내용 |
|---|---|
| 파일·줄 | `task_status.py:334`에서 `db_support.library_stats()` 호출 → `db_support.py:121` |
| 현재 동작 | `metadata_items`를 `GROUP BY library_id, metadata_type`으로 집계한다. 작품(main)·하위(sub)·묶음(group) 수와, 작품 기준 메타 누락(`no_code`·`no_summary`·`no_year`·`no_thumb`·`missing_any`)을 센다. `media_parts` 조인으로 영화 파일 수·쇼/도서 하위 파일 수도 센다. 폴더 에이전트 라이브러리는 표지 기준으로 건강도를 판단한다 (`lib_variants`, `ff_module_name`/`scanner_variant` 참조) |
| Relay에서 읽는 것 | 라이브러리별 항목 수·메타 누락 수·파일 수·휴지통 수·적용 에이전트 이름 |
| API 대체 | **불가** — 타입별 항목 수·메타 누락 수를 세어 주는 집계 API가 없다. `/statistics`는 재생 통계라 해당 없음. 전수 조회로 세는 것도 라이브러리 규모(수만 건)상 비현실적 |
| 대체 API | 없음 |

### 1-3. 건강도 종합 집계 — API 대체 불가 (Relay 유지)

| 항목 | 내용 |
|---|---|
| 파일·줄 | `task_status.py:336`에서 `db_support.dashboard_stats(libs)` 호출 → `db_support.py:291` |
| 현재 동작 | `library_stats()` 결과를 합산만 하는 순수 계산이다. DB를 직접 읽지 않는다 |
| API 대체 | **불가** — 입력 자체가 1-2의 Relay 집계라, 입력이 없으면 계산할 수 없다 |
| 대체 API | 없음 |

### 1-4. 도서 실측 파일 수 — 부분 가능

| 항목 | 내용 |
|---|---|
| 파일·줄 | `db_support.py:261` `book_file_count(section_key)` — 호출처 `task_periodic.py:108` `book_relay_total()` |
| 현재 동작 | 도서 라이브러리 1개의 `media_parts` 수(타입 102)를 센다. 도서 스캔 stats의 `entries`는 이번 스캔 처리 수(델타)라 '전체' 표시용으로 실측값을 쓴다 |
| API 대체 | **부분 가능** — 도서 '책 수'로 근사 가능. 목록 API의 전체 개수(total)를 쓸 수 있는지 확인이 필요하다. 안 되면 Relay 유지 또는 entries 델타에 '처리' 라벨(현행 폴백) 유지 |

---

## 2. 휴지통 조회 — `task_trash_view.py` + `db_support.trash_compute`

| 항목 | 내용 |
|---|---|
| 파일·줄 | `task_trash_view.py:267` `compute_view()` → `:286`에서 `db_support.trash_compute(lib_id)` 호출 |
| 현재 동작 | `db_support.py:498` `_trash_load()`가 (1) `media_parts`에서 해당 라이브러리의 `deleted_at IS NOT NULL` 파일 전수(id·metadata_item_id·file_path·deleted_at·deleted_reason)를 읽고, (2) `metadata_items`에서 파일이 속한 항목부터 최상위까지 parent 체인을 따라 올라가 항목 사전을 만든다. `db_support.py:710` `trash_compute()`는 이를 작품(main) 단위(음악=앨범, 영화=영화, 쇼=쇼, 도서=시리즈)로 묶고, 그룹별 파일 목록·첫/마지막 삭제 시각·사유·공통 폴더·보관 기간(`trash_retention_days()`, `:452`)까지 계산해 캐시(`trash_view_cache`)에 저장한다. 웹은 캐시에서 검색·정렬·페이지만 처리한다 |
| Relay에서 읽는 것 | 휴지통 파일 목록, 항목 계층(parent_id 체인), 항목 제목·연도, 보관 기간 |
| API 대체 | **가능 (조건부)** — `GET /libraries/{library_id}/trash`로 파일 목록·개수·삭제 시각·사유·폴더는 얻을 수 있다. 보관 기간은 `GET /libraries/{library_id}`(라이브러리 상세)로 대체 가능 |
| 조건·주의 | 현재 그룹핑은 parent_id 체인으로 작품 단위를 묶고 부모 제목(아티스트)·연도를 표시한다. `/trash` API 응답에 `metadata_item_id`·`parent_id`가 없으면 (1) `folder_path` 기준 그룹핑으로 바꾸거나 (2) 항목별 `GET /library/metadata/{id}` 추가 조회가 필요하다. (1)은 표시 형태가 바뀌고, (2)는 파일 수만큼 API를 불러 느려진다. 구현 전 응답 필드 확인이 먼저다 |
| 같이 바뀌는 것 | `task_trash_view.py:297` `set_trash_count()` — 조회 결과를 개수 캐시에 반영하는 부분은 API 경로에서도 그대로 유지 가능. `db_support.py:625` `trash_groups()`도 같은 `_trash_load()` 원자료를 쓰므로 같은 조건이 적용된다 (단, 현재 실제 호출처는 없고 `trash_compute`만 쓰이고 있다) |

---

## 3. 스캔 탭 휴지통 게이트 — `mod_scan.py` db_ready 5곳

| 줄 | 명령 | 현재 동작 | API 대체 |
|---|---|---|---|
| `:227`~`:228` | `trash_counts` | `trash_counts_for_stats()`로 휴지통 개수를 돌려준다 (라이브러리 선택 팝업용) | **가능** — 1-1과 같은 방식으로 개수를 API로 구하고, 게이트를 관리자 세션 확인으로 교체 |
| `:240`~`:241` | `trash_query` | `task_trash_view.request_view()`로 휴지통 조회 계산을 워커에 요청한다 | **가능 (조건부)** — 2번 항목과 동일 조건 |
| `:252`~`:253` | `trash_query_all` | `task_trash_view.request_bulk()`로 여러 라이브러리 전체 조회를 요청한다 | **가능 (조건부)** — 2번 항목과 동일 조건 |
| `:279`~`:280` | `trash_list` | 워커가 계산해 둔 캐시를 검색·정렬·페이지로 잘라 돌려준다. DB를 직접 읽지 않는다 | **게이트만 교체** — 캐시 읽기만 하므로, 계산이 API로 바뀌면 게이트를 관리자 세션 확인으로 바꾸는 것만으로 충분 |
| `:308`~`:309` | `trash_files` | 캐시에서 한 작품의 파일 목록을 돌려준다. DB를 직접 읽지 않는다 | **게이트만 교체** — `trash_list`와 동일 |

참고: `trash_empty_all`(`mod_scan.py:333`)은 이미 compat API(`GET /library/sections/{id}/emptyTrash`)라
게이트가 없고 손댈 필요가 없다. 다섯 곳의 실패 문구('Relay에서 연결 테스트를 통과해야…')도
관리자 API 기준으로 바꿔야 한다.

---

## 4. 앨범 정보 표시 — `shyni_backend.py` + `db_support.metadata_info`

### 4-1. `_apply_album_info` — API 대체 가능 (조건부)

| 항목 | 내용 |
|---|---|
| 파일·줄 | `shyni_backend.py:341` `_apply_album_info(db_item, metadata_id)` → `:346`에서 `db_support.metadata_info()` 호출 |
| 현재 동작 | 스캔 항목의 `meta_info`에 앨범 정보를 채워 목록에 표시한다. 제목·연도·요약·표지(`thumb_url`)를 넣고, 가수면 앨범 수·트랙 수도 넣는다. 호출처는 `_refresh_metadata()`(`:574` 부근)와 `meta_refresh_now()`(`:564` 부근) |
| Relay에서 읽는 것 | `db_support.py:405` `metadata_info()` — `metadata_items` 1건(유형·부모·제목·연도·요약·표지). 트랙(타입 10)이면 부모 앨범 정보로 교체하고, 가수(타입 8)면 앨범 수·트랙 수를 `COUNT(*)`로 센다 |
| API 대체 | **가능 (조건부)** — `GET /library/metadata/{id}`로 제목·연도·요약·표지·유형·부모 id를 얻을 수 있으면 트랙→앨범 교체까지 가능하다. 가수 앨범/트랙 수는 자식 목록(children) API 또는 음악 API로 확인이 필요하다 |
| 대체 API | `GET /library/metadata/{id}` (+ 자식 목록 API, 확인 필요) |

### 4-2. `meta_refresh_now` 변경 확인 폴링 — API 대체 가능

| 항목 | 내용 |
|---|---|
| 파일·줄 | `shyni_backend.py:520` `meta_refresh_now()` — `:540`에서 변경 전 정보(`old`)를 읽고, `:557`에서 180초간 10초 간격으로 변경 후 정보(`now`)를 다시 읽어 바뀐 것을 확인한다 |
| 현재 동작 | 메타 새로고침(PUT `/library/metadata/{id}/refresh`, compat 토큰 — 이미 API)을 보낸 뒤, shyni가 DB에 반영할 때까지 기다렸다가 최신 정보로 채운다. `db_ready()`가 거짓이면 폴링 없이 20초만 기다린다 |
| API 대체 | **가능** — 변경 전/후 비교를 `GET /library/metadata/{id}`로 똑같이 할 수 있다 |
| 대체 API | `GET /library/metadata/{id}` |

참고: `metadata_id`를 얻는 경로(`/compat/metadata_by_path`, `shyni_backend.py:416`·`:443`·`:530`·`:574`)는
이미 compat API라 Relay와 무관하다. Relay에 의존하는 것은 id를 안 뒤의 정보 조회뿐이다.

---

## 5. 추가 파일 묶음 — `model_trash.py` + `db_support.group_append_paths`

### 5-1. `group_append_paths` 자체 — Relay와 무관 (손댈 필요 없음)

| 항목 | 내용 |
|---|---|
| 파일·줄 | `db_support.py:934` `group_append_paths(paths)` — 순수 함수 |
| 현재 동작 | 파일 경로 문자열 목록을 폴더(앨범) 단위로 묶어 `[{folder, name, count, files}]`를 돌려준다. DB 접속이 전혀 없다. `db_support.py`에 있다는 이유로 import할 뿐이다 |
| 사용처 | `model_trash.py:241`(추가 파일)·`:255`(비운 파일)·`:276`(대상 파일 + 폴더별 VFS 결과 병합)·`:295`(복원된 파일) — 모두 mate 자체 SQLite 이력 행에 저장된 JSON 목록을 표시용으로 묶는 것이다 |
| API 대체 | 해당 없음 — Relay를 쓰지 않으니 Step4에서 손댈 필요가 없다 |

### 5-2. 입력 목록의 출처 — API 대체 가능 (별도 작업)

`group_append_paths`는 순수하지만, 그 입력인 파일 목록 중 일부는 Relay에서 온다.
이 목록들의 출처를 API로 바꾸는 것이 Step4의 실제 작업이다.

| 줄 | 목록 | 현재 출처 | API 대체 |
|---|---|---|---|
| `task_trash.py:774` | 대상 파일 (`target_files`) | `db_support.trash_file_paths(key)` (`db_support.py:545` — `media_parts`에서 휴지통 경로만 조회) | **가능** — `GET /libraries/{library_id}/trash`의 `file_path` 목록. Relay가 꺼져 있으면 지금은 작업 자체가 건너뛰어지므로(`:776`~`:788`), API 전환이 휴지통 작업의 Relay 의존을 푸는 핵심이다 |
| `task_trash.py:933` | 스캔 후 파일 (`post_files`, 복원 판정용) | 같은 `trash_file_paths()` — 스캔 전/후 목록 비교로 복원된 파일을 가려낸다 | **가능** — 같은 API |
| `task_trash.py:952` | 비운 파일 (`emptied_files`) | 같은 `trash_file_paths()` — 비우기 전에 미리 읽어둔다 | **가능** — 같은 API |
| `task_trash.py:799` | 대상 폴더 묶음 | `group_append_paths(target_files)` — 입력만 API로 바뀌면 그대로 동작 | 해당 없음 (순수 함수) |

### 5-3. `model_trash.py:319`~`:326` `db_ready` 게이트

실행 중 목록 문구용으로 '목록 저장 가능 여부'를 내려준다. API 전환 시 이 판정 기준을
관리자 API 가용성(세션 토큰 있음)으로 바꿔야 한다.

---

## 6. 스캔 대기 로직 — `task_periodic.py` · `task_trash.py`

### 6-1. 현재 구조 — compat job 폴링만 한다

| 항목 | 내용 |
|---|---|
| 주기적 스캔 | `task_periodic.py:958` `_poll_scan_job()` — 10초(`SCAN_POLL_SEC`, `:39`)마다 `GET /compat/jobs/{job_id}`(X-Plex-Token)를 불러 `_scan_poll_outcome()`(`:161`)으로 `completed`/`failed`만 판정한다. 진행률은 전혀 보지 않는다. 호출처는 `_do_scan()`(`:955`)과 재시작 복구 `_resume()`(`:1063`) |
| 휴지통 작업 | `task_trash.py:421` `_wait_job()` — 같은 구조, 20초(`SCAN_POLL_SEC`, `:37`) 폴링. `_wait_scan()`(`:473`)은 래퍼. 비우기 job 대기(`:985`)도 같은 함수를 쓴다 |
| 대시보드 스캔 | `shyni_backend.py:658` `_poll_item()` — 개별 스캔 항목도 `GET /compat/jobs/{job_id}`(`:666`)로만 확인한다. `stats.phase`는 조립 당시 값으로 고정돼 스캔 시작 후에도 `probe_assembling`으로 남는 실측이 있다 (`:687`~`:695` 주석) |
| 선택 실행 | `task_trash_selected.py:157` `_do_scan()`은 전송만 하고 대기하지 않는다 (폴링 없음) |
| 기한 | `WAIT_TIMEOUT_SEC = 6 * 3600` (`task_periodic.py:36`). VFS 대기·스캔 대기·휴지통 대기가 같은 6시간을 쓴다. 기한이 지나면 mate가 기다리기를 그만둘 뿐 shyni 스캔 자체는 계속 돈다 |

### 6-2. 스캔 상태 API로 진행률을 보려면 고칠 곳

| 고칠 곳 | 내용 |
|---|---|
| `task_periodic.py:958` `_poll_scan_job()` | `section_key` 인자를 추가한다. 호출처 `_do_scan()`(`:933`)은 `sec['key']`를, `_resume()`(`:1063`)은 `item.section_key`를 갖고 있어 전달 가능하다. 폴링 루프 안에서 `admin_get(f'/libraries/{section_key}/scan/status')`를 주기적으로 불러 `processed/total`·`stage`·`current_file`을 현재 상태(`_write_status()` / `get_current()`)에 기록한다 |
| `task_periodic.py:964` 기한 판정 | 진행 기반 기한으로 바꾸려면 `processed`가 전진할 때마다 `deadline`을 연장하거나, 별도의 정체 타이머(예: N분간 전진 없음)를 둔다. job 폴링은 완료 판정의 정본으로 유지한다 |
| `task_trash.py:421` `_wait_job()` / `:473` `_wait_scan()` | 같은 방식으로 section key를 받는다. 호출처 `:897`은 `key`를 갖고 있다. 단, 비우기 job 대기(`:985`)는 스캔이 아니므로 진행률 폴링 대상에서 제외한다 |
| `shyni_backend.py:658` `_poll_item()` | 항목별 부분 스캔이라 라이브러리 단위 진행률과 직접 대응하지 않는다. Step4 대상에서는 제외하고, 필요하면 나중에 참고용으로만 붙인다 |

### 6-3. 주의 사항

- `scan/status`는 **라이브러리 단위** 서버 메모리 상태라 job 단위가 아니다. 같은 라이브러리를
  동시에 스캔하면 상태가 공유되고, job이 끝나면 idle이 될 수 있다. 그래서 완료 판정은
  기존 compat job 폴링이 계속 맡고, 상태 API는 진행 표시·정체 판정용으로만 쓴다.
- 상태 API는 관리자 세션이 필요하다 (`admin_get` 사용). Relay와 무관하게 동작한다.
- VFS 대기(`_wait_vfs`, rclone RC)는 스캔 상태 API와 무관하니 건드리지 않는다.

---

## 부록. 그 외 Relay 사용처 (결론 표 완성을 위해 확인한 것)

| 위치 | 기능 | API 대체 |
|---|---|---|
| `mod_status.py:361` `_db_scan_history()` | `compat_jobs` 테이블에서 라이브러리별 최신 전체 스캔 기록을 읽는다 (대시보드 '최근 스캔' 표시) | **미확인** — job 목록 API가 있는지 확인 필요. 없으면 Relay 유지. Relay OFF 시 이미 로그 파싱으로 폴백(`:417`)하므로 기능 자체는 유지되고 성능만 저하된다 |
| `mod_status.py:1232`~`:1246` 메타 새로고침 | `album_ids()`(`db_support.py:367`)로 새로고침 대상 앨범 id 목록을 읽는다. `library_is_folder()`(`:348`)·`library_main_label()`(`:327`)도 함께 쓴다 | **부분 가능** — 코드 없는 앨범은 `filter=unmatched` API로, 표지 없는 앨범은 전수 조회로. 라이브러리 종류는 `GET /library/sections`로 가능. 폴더 에이전트 판정은 sections 응답에 에이전트 필드가 있는지 확인 필요 |
| `mod_status.py:1701` `_check_meta_filled()` | `count_filled()`(`db_support.py:384`)로 새로고침 후 실제로 채워진 항목 수를 센다 | **부분 가능** — 항목별 `GET /library/metadata/{id}` 전수 조회. 대상이 많으면 느리다 |
| `db_support.py:824` `parts_max_id()` 외 추가 파일 수집 일체 (`paths_by_metadata_item_ids` `:836`, `count_parts_newer_than` `:862`, `paths_newer_than` `:875`, `collect_append_paths` `:894`) | 스캔 전후 `MAX(media_parts.id)` 비교로 스캔 후 추가된 파일 목록을 구한다. 호출처: `task_periodic.py:247`·`:773`, `mod_status.py:570`·`:613` | **불가** — 대체 API 없음. 이미 Relay OFF면 `(None, None)`을 돌려 개수까지만 표시하는 폴백이 있다 |
| `db_support.py:452` `trash_retention_days()` | 라이브러리 보관 기간 (화면 표시용) | **가능 (조건부)** — 라이브러리별 값은 `GET /libraries/{library_id}`로 가능. 전역 폴백(`server_settings` config)은 전역 설정 API 확인 필요. 없으면 기본 7일 유지 |

---

## 결론 — Step4에서 API로 바꿀 항목과 Relay로 남길 항목

### API로 바꾸는 항목

| 항목 | 위치 | 대체 API |
|---|---|---|
| 휴지통 개수 | `task_status.py:264` `trash_counts_for_stats()` | `GET /libraries/{library_id}/trash` (목록 길이) |
| 휴지통 목록·상세 조회 | `task_trash_view.py:286` → `db_support.trash_compute()` | `GET /libraries/{library_id}/trash` + `GET /libraries/{library_id}`(보관 기간). ※ 그룹핑 방식 조정 필요 (응답 필드 확인 후) |
| 휴지통 대상·복원·비운 파일 목록 | `task_trash.py:774`·`:933`·`:952` `trash_file_paths()` | `GET /libraries/{library_id}/trash` (`file_path` 목록) |
| 스캔 탭 휴지통 게이트 5곳 | `mod_scan.py:228`·`:241`·`:253`·`:280`·`:309` | 게이트를 관리자 세션 확인으로 교체 (`trash_list`·`trash_files`는 게이트만) |
| 앨범 정보 표시·변경 확인 | `shyni_backend.py:346`·`:540`·`:557` `metadata_info()` | `GET /library/metadata/{id}` |
| 라이브러리 종류·작품 단위 | `db_support.py:327` `library_main_label()` | `GET /library/sections` |
| 스캔 진행률 표시 (신규) | `task_periodic.py:958` `_poll_scan_job()`, `task_trash.py:421` `_wait_job()` | `GET /libraries/{id}/scan/status` (진행 표시·정체 판정용, 완료 판정은 기존 job 폴링 유지) |
| 휴지통 작업 Relay 의존 해제 | `task_trash.py:776`~`:788` (Relay OFF면 작업 건너뜀) | 위 `trash_file_paths()` API 전환으로 자동 해소 |
| 실행 중 목록 문구 게이트 | `model_trash.py:324` `db_ready` | 관리자 API 가용성 판정으로 교체 |

### 부분 가능 항목 (API로 일부만, 나머지는 Relay 또는 폴백)

| 항목 | 위치 | 내용 |
|---|---|---|
| 메타 없는 앨범 목록 | `mod_status.py:1244` `album_ids()` | 코드 없음은 `filter=unmatched`로, 표지 없음은 전수 조회로. 전수 조회가 부담되면 Relay 유지 |
| 메타 반영 확인 | `mod_status.py:1709` `count_filled()` | 항목별 metadata API 전수 조회. 대상이 많으면 느리니 횟수 제한 또는 Relay 유지 검토 |
| 도서 실측 파일 수 | `db_support.py:261` `book_file_count()` | 책 수로 근사 (목록 total 확인 필요). 안 되면 entries 델타 + '처리' 라벨 폴백 유지 |
| 폴더 에이전트 판정 | `db_support.py:348` `library_is_folder()` | sections 응답에 에이전트 필드가 있으면 가능, 없으면 Relay 유지 |
| 보관 기간 전역 폴백 | `db_support.py:452` | 라이브러리별 값은 API로, 전역 설정은 API 확인 필요 |
| 최근 스캔 이력 | `mod_status.py:361` `_db_scan_history()` | job 목록 API 확인 필요. 없으면 로그 파싱 폴백 유지 (Relay OFF 시 이미 폴백됨) |

### Relay로 남기는 항목

| 항목 | 위치 | 이유 |
|---|---|---|
| 대시보드 통계·건강도 집계 | `db_support.py:121` `library_stats()`, `:291` `dashboard_stats()` | 집계 API 없음. 전수 조회로 세기에는 규모가 너무 큼 |
| 스캔 후 추가 파일 목록 | `db_support.py:894` `collect_append_paths()` 일체 (`parts_max_id`·`paths_newer_than`·`paths_by_metadata_item_ids`) | `MAX(id)` 비교 대체 API 없음. Relay OFF 폴백(개수까지만 표시) 이미 있음 |
| 카드 스캔 신규 측정 | `mod_status.py:567`·`:610` | 같은 이유 (`collect_append_paths` 재사용) |

### 손댈 필요 없는 항목

| 항목 | 위치 | 이유 |
|---|---|---|
| `group_append_paths()` | `db_support.py:934`, 사용처 `model_trash.py:241`·`:255`·`:276`·`:295`, `task_trash.py:799` | 순수 함수. DB 접속 없음. 입력 목록의 출처만 API로 바뀌면 그대로 동작한다 |
| 휴지통 비우기·스캔 전송·메타 새로고침 전송 | `mod_scan.py:333`, `task_periodic.py:939`, `shyni_backend.py:541`·`:590` 등 | 이미 compat API(X-Plex-Token)로 동작 중 |
| `metadata_id` 조회 | `shyni_backend.py:416`·`:443`·`:530`·`:574` `/compat/metadata_by_path` | 이미 API |
| 대시보드 목록의 섹션 정보 | `mod_status.py:245` `GET /library/sections` | 이미 API (compat 토큰) |
| VFS 새로고침 대기 | `task_periodic.py:877`·`task_trash.py:392` `_wait_vfs()` | rclone RC라 스캔 상태 API와 무관 |

### 구현 순서 제안

1. **휴지통 계열 먼저** (`trash_counts` → `trash_file_paths` → `trash_compute` → `mod_scan.py` 게이트 5곳) — API 대체가 가장 확실하고, 휴지통 작업의 Relay 의존을 푸는 효과가 크다. 단, `trash_compute` 그룹핑은 `/trash` 응답 필드 확인이 선행되어야 한다.
2. **앨범 정보** (`metadata_info`) — 단일 항목 조회라 범위가 작고, `meta_refresh_now` 폴링까지 함께 바꿀 수 있다.
3. **스캔 진행률** (`_poll_scan_job`·`_wait_job`에 상태 API 추가) — 기존 job 폴링을 건드리지 않고 곁에 붙이는 형태라 리스크가 낮다. 진행 기반 기한(6시간 고정 대신 정체 판정)은 진행률 표시가 안정된 뒤에 붙인다.
4. **부분 가능 항목** — `album_ids`·`count_filled`·`book_file_count`는 API 응답·성능을 확인하면서 하나씩 판단한다.
5. **Relay 유지 항목** — 손대지 않는다. 대시보드 통계·건강도와 추가 파일 목록은 Relay ON일 때만 표시된다는 안내가 이미 0.9.8i에 있다.
