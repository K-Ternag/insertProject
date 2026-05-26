# workflow_records_tracker

`workflow_records.json`을 읽어서 이전 실행 이후 새로 추가된 `new_records`만 출력하는 테스트용 스크립트입니다.

## 실행 방법

### 1회 실행

```powershell
python D:\CompPjts\insertFiles\workflow_records_tracker.py
```

### 1분마다 반복 실행

```powershell
python D:\CompPjts\insertFiles\workflow_records_tracker.py --loop --interval-seconds 60
```

### 테스트용으로 1회만 반복 모드 실행

```powershell
python D:\CompPjts\insertFiles\workflow_records_tracker.py --loop --interval-seconds 60 --iterations 1
```

## 동작

- 각 `D:\CompPjts\crawlFilesDev\outputs\<folder>\filter\workflow_records.json`을 읽습니다.
- 이전 checkpoint 이후 새로 앞에 추가된 record만 찾습니다.
- checkpoint는 `records[*].final_url`을 사용합니다.
- state는 현재 `outputs` 아래 실제 폴더만 남기고 나머지는 저장 전에 삭제합니다.
- state에 없던 새 폴더가 처음 발견되면 해당 시점의 `records`를 출력하고 checkpoint를 저장합니다.
- 각 실행마다 `workflow_records_checkpoint_history.log`에 이전값/새값을 append-only로 남깁니다. 값이 같으면 `checkpoint_unchanged`, 다르면 `checkpoint_changed`로 남깁니다.
- state에 실제로 저장된 값은 `state_persisted`로 한 번 더 남깁니다.
- `new_records`가 있으면 폴더별로 `search_terms`, `filter_terms`, `new_records`를 함께 `print`합니다.
- `tempfunc` 입력 형태는 아래와 같습니다.

```json
{
  "naver_news": {
    "search_terms": ["프랑스", "독일"],
    "filter_terms": ["프랑스"],
    "new_records": [
      {
        "record_key": "term001_item001",
        "success": true,
        "extracts": {},
        "downloaded_files": [],
        "extracted_files": [],
        "error": null,
        "start_url": "https://openapi.naver.com/v1/search/news.json?query=...",
        "final_url": "https://n.news.naver.com/...",
        "output_file": "outputs\\naver_news\\filter\\..."
      }
    ]
  }
}
```
- 중복 실행은 `workflow_records_tracker.lock` 파일 잠금으로 막습니다.

## 주요 옵션

- `--outputs-root`: 입력 폴더 루트 변경
- `--state-file`: checkpoint 저장 파일 변경
- `--lock-file`: 잠금 파일 경로 변경
- `--error-log-file`: 루프 모드 오류/스킵 로그 파일 변경
- `--checkpoint-log-file`: `workflow_records_state.json`의 `checkpoint_key` 변경 이력 로그 파일 변경
- `--no-initialize-missing`: 아직 checkpoint가 없는 폴더를 baseline 처리하지 않음
- `--loop`: 계속 반복 실행
- `--interval-seconds`: 반복 간격
- `--iterations`: 반복 횟수 제한
