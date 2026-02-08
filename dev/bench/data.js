window.BENCHMARK_DATA = {
  "lastUpdate": 1770566126040,
  "repoUrl": "https://github.com/izure1/serializable-bptree",
  "entries": {
    "Serializable B+Tree Benchmark": [
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "03b780d3b137155ae411d1f21a3b53f2a547b4f9",
          "message": "chore: 벤치마킹 결과는 커밋되지 않도록 수정합니다",
          "timestamp": "2026-02-09T00:53:45+09:00",
          "tree_id": "b1ecc32844ad043896d69e19a026432f0880bd67",
          "url": "https://github.com/izure1/serializable-bptree/commit/03b780d3b137155ae411d1f21a3b53f2a547b4f9"
        },
        "date": 1770566125656,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Async Stream Scan",
            "value": 167,
            "unit": "ms"
          },
          {
            "name": "Point Query latency",
            "value": 15,
            "unit": "ms"
          },
          {
            "name": "Sync Where latency",
            "value": 17,
            "unit": "ms"
          },
          {
            "name": "MVCC Conflict overhead",
            "value": 67,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}