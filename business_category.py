"""
업종 분류 유틸리티
일반 업종(place, hospital 등)과 맛집 업종(restaurant)을 구분
"""


def classify_business_category(tab_title: str = "", product: str = "", product_name: str = "") -> str:
    """업종 분류 (일반/맛집)

    Args:
        tab_title: 시트 탭 제목
        product: 상품 컬럼 값
        product_name: 상품명 컬럼 값

    Returns:
        "맛집" 또는 "일반"
    """
    # 텍스트 정규화 (소문자 + 공백 제거)
    def normalize(text: str) -> str:
        return (text or "").strip().lower().replace(" ", "")

    tab_norm = normalize(tab_title)
    product_norm = normalize(product)
    product_name_norm = normalize(product_name)

    # 맛집 키워드
    restaurant_keywords = [
        "맛집", "restaurant", "카페", "cafe", "음식점",
        "치킨", "피자", "파스타", "일식", "중식", "한식", "양식",
        "브런치", "디저트", "베이커리"
    ]

    # 탭 제목, product, product_name 중 하나라도 맛집 키워드 포함 시 맛집
    all_text = f"{tab_norm} {product_norm} {product_name_norm}"

    for keyword in restaurant_keywords:
        if keyword in all_text:
            return "맛집"

    # 기본값: 일반
    return "일반"


def group_workload_by_category_and_agency(workload_data: dict) -> dict:
    """작업량 데이터를 업종별 → 대행사별로 그룹화

    Args:
        workload_data: {
            "weeks": [
                {
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-07",
                    "items": [
                        {
                            "name": "작업명",
                            "workload": 10,
                            "tab_title": "탭명",
                            "product": "상품",
                            "product_name": "상품명",
                            "agency": "대행사명"
                        }
                    ]
                }
            ]
        }

    Returns:
        {
            "일반": {
                "대행사A": {
                    "weeks": [...]
                },
                "대행사B": {
                    "weeks": [...]
                }
            },
            "맛집": {
                "대행사A": {
                    "weeks": [...]
                },
                "대행사B": {
                    "weeks": [...]
                }
            }
        }
    """
    result = {
        "일반": {},
        "맛집": {}
    }

    weeks = workload_data.get("weeks", [])

    for week in weeks:
        start_date = week.get("start_date")
        end_date = week.get("end_date")

        for item in week.get("items", []):
            # 업종 분류
            category = classify_business_category(
                tab_title=item.get("tab_title", ""),
                product=item.get("product", ""),
                product_name=item.get("product_name", "")
            )

            # 대행사명 (없으면 "미지정")
            agency = item.get("agency", "").strip() or "미지정"

            # 대행사별 그룹 생성
            if agency not in result[category]:
                result[category][agency] = {"weeks": []}

            # 해당 기간이 이미 있는지 확인
            agency_weeks = result[category][agency]["weeks"]
            existing_week = None
            for w in agency_weeks:
                if w.get("start_date") == start_date and w.get("end_date") == end_date:
                    existing_week = w
                    break

            # 기간이 없으면 새로 생성
            if not existing_week:
                existing_week = {
                    "start_date": start_date,
                    "end_date": end_date,
                    "items": []
                }
                agency_weeks.append(existing_week)

            # 작업 추가
            existing_week["items"].append({
                "name": item.get("name", ""),
                "workload": item.get("workload", 0)
            })

    # 빈 대행사 그룹 제거
    for category in ["일반", "맛집"]:
        result[category] = {
            agency: data
            for agency, data in result[category].items()
            if data["weeks"]
        }

    return result


def format_grouped_workload_text(grouped_data: dict, business_name: str = "", guarantee_rank: str = "") -> str:
    """업종별 → 대행사별 그룹화된 작업량 데이터를 텍스트로 변환

    Args:
        grouped_data: group_workload_by_category_and_agency() 결과
        business_name: 상호명
        guarantee_rank: 보장순위

    Returns:
        포맷팅된 텍스트

    Example Output:
        월보장 (3위)

        === 일반 업종 ===

        [대행사A]
        2025-01-01 ~ 2025-01-07
        작업명1 : 10
        작업명2 : 5

        [대행사B]
        2025-01-08 ~ 2025-01-14
        작업명3 : 8

        === 맛집 업종 ===

        [대행사A]
        2025-01-01 ~ 2025-01-07
        작업명4 : 12
    """
    lines = []

    # 헤더 (월보장)
    if business_name:
        rank = ""
        if guarantee_rank:
            rank_str = str(guarantee_rank)
            rank = f"({rank_str})" if "위" in rank_str else f"({rank_str}위)"
        lines.append(f"월보장 {rank}")
        lines.append("")  # 빈 줄

    # 업종별 처리
    for category in ["일반", "맛집"]:
        agencies = grouped_data.get(category, {})

        if not agencies:
            continue  # 해당 업종에 작업이 없으면 스킵

        # 업종 헤더
        lines.append(f"=== {category} 업종 ===")
        lines.append("")

        # 대행사별 처리
        for agency, data in sorted(agencies.items()):
            lines.append(f"[{agency}]")

            weeks = data.get("weeks", [])
            for idx, week in enumerate(weeks):
                # 날짜 범위
                start_date = week.get("start_date")
                end_date = week.get("end_date")

                if start_date:
                    date_range = f"{start_date} ~ {end_date}"
                else:
                    date_range = f"~ {end_date}"

                lines.append(date_range)

                # 작업 항목
                for item in week.get("items", []):
                    name = item.get("name", "")
                    workload = item.get("workload", 0)
                    lines.append(f"{name} : {workload}")

                # 주차 사이 빈 줄 (마지막 주차가 아니면)
                if idx < len(weeks) - 1:
                    lines.append("")

            lines.append("")  # 대행사 사이 빈 줄

        lines.append("")  # 업종 사이 빈 줄

    return "\n".join(lines).strip()
