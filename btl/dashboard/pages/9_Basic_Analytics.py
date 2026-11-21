"""Basic descriptive analysis from a single read-only warehouse snapshot."""

import pandas as pd
import plotly.express as px
import streamlit as st
from analytics.basic import build_basic_report, source_groups
from dashboard import ui
from dashboard.experience import options

ui.setup_page("Phân tích cơ bản", "Thống kê, so sánh và đọc lịch sử giá từ dữ liệu đã thu thập.", "basic")
with ui.warehouse() as db:
    products = db.read_table("products")
    reviews = db.read_table("reviews")
    prices = db.read_table("price_daily_stats")
if products.empty:
    ui.empty_state(
        "Chưa có dữ liệu để phân tích",
        "Cần danh mục sản phẩm trong phạm vi đang chọn. Trang này chỉ đọc dữ liệu đã lưu.",
    )
    ui.footer()
    st.stop()
with st.container(border=True):
    groups = source_groups(products, reviews, prices)
    source = st.selectbox(
        "Nhóm nguồn",
        groups,
        key="basic_source",
        format_func=lambda x: {
            "marketplace": "Thương mại điện tử",
            "simulation": "Giả lập có nhãn",
            "web_content": "Nội dung web",
            "unverified": "Chưa kiểm chứng",
        }.get(x, x),
    )
    scope = products.loc[
        products.get("source_group", pd.Series(index=products.index, dtype=object))
        .fillna("unverified")
        .replace("", "unverified")
        .eq(source)
    ]
    left, right, third = st.columns(3)
    brands = left.multiselect("Thương hiệu", options(scope, "brand"), key="basic_brands")
    categories = right.multiselect("Danh mục", options(scope, "category"), key="basic_categories")
    sellers = third.multiselect("Nhà bán", options(scope, "seller"), key="basic_sellers")
    dates = []
    for frame, column in ((reviews, "review_date"), (prices, "date")):
        if column in frame:
            frame = frame.loc[
                frame.get("source_group", pd.Series(index=frame.index, dtype=object))
                .fillna("unverified")
                .replace("", "unverified")
                .eq(source)
            ]
            dates.extend(
                pd.to_datetime(frame[column], errors="coerce", utc=True, format="mixed").dropna().dt.date.tolist()
            )
    bounded = st.checkbox("Lọc theo khoảng thời gian", key="basic_date_filter", disabled=not dates)
    start = end = None
    if bounded and dates:
        selected = st.date_input("Khoảng ngày quan sát (UTC)", value=(min(dates), max(dates)), key="basic_dates")
        if len(selected) != 2:
            st.info("Chọn đủ ngày bắt đầu và kết thúc.")
            st.stop()
        start, end = selected
    st.caption(
        "Bộ lọc ngày áp dụng cho đánh giá và lịch sử giá. Sản phẩm, thương hiệu và giá danh mục là trạng thái hiện đang được lưu; đây không phải tồn kho hay doanh số lịch sử."
    )
try:
    report = build_basic_report(
        products,
        reviews,
        prices,
        source_group=source,
        brands=brands,
        categories=categories,
        sellers=sellers,
        start_date=start,
        end_date=end,
    )
except ValueError as error:
    st.error(str(error))
    st.stop()
summary = report.summary
for column, (title, value, note) in zip(
    st.columns(4),
    [
        ("Sản phẩm", ui.number(summary["products"]), "Danh mục hiện tại sau bộ lọc"),
        ("Đánh giá đã thu", ui.number(summary["reviews"]), "Bản ghi khớp danh mục và bộ lọc"),
        ("Điểm trung bình", ui.number(summary["avg_rating"], 2), f"{summary['valid_ratings']:,} mẫu điểm hợp lệ"),
        (
            "Tỷ lệ điểm 4–5",
            f"{summary['positive_ratio']:.1%}" if summary["positive_ratio"] is not None else "Chưa có dữ liệu",
            "Phân nhóm theo sao, không suy luận cảm xúc văn bản",
        ),
    ],
):
    with column:
        ui.metric(title, value, note)
st.write("")
if summary["products"]:
    with st.container(border=True):
        st.subheader("Độ bao phủ của mẫu")
        rated = report.coverage["rated_products"]
        total = summary["products"]
        st.write(f"**{rated}/{total} sản phẩm** có đánh giá hợp lệ trong bộ lọc hiện tại.")
        seller_known = report.coverage["catalog_seller_known"]
        st.write(f"**{seller_known}/{total} sản phẩm** có thông tin nhà bán trong danh mục hiện tại.")
        if rated > 3:
            top = report.coverage["top_three_ratings"]
            samples = summary["valid_ratings"]
            st.write(f"Ba sản phẩm có nhiều điểm nhất đóng góp **{top}/{samples} lượt chấm điểm "
                     f"({top / samples:.1%})** vào điểm trung bình.")
        comparable = report.coverage["price_comparable_products"]
        st.write(f"**{comparable}/{total} sản phẩm** có ít nhất hai ngày quan sát giá trong cùng tiền tệ để so sánh.")
comparison_tab, reviews_tab, price_tab = st.tabs(["So sánh & thống kê", "Đánh giá & xu hướng", "Lịch sử giá"])
with comparison_tab:
    with st.container(border=True):
        dimension = st.selectbox(
            "So sánh theo",
            ["brand", "category", "seller"],
            format_func=lambda v: {"brand": "Thương hiệu", "category": "Danh mục", "seller": "Nhà bán"}[v],
            key="basic_dimension",
        )
        comparison = {"brand": report.brands, "category": report.categories, "seller": report.sellers}[dimension]
        if dimension == "seller":
            st.caption("Nhóm theo nhà bán trong danh mục hiện tại. Điểm thu được phản ánh đánh giá sản phẩm.")
        if comparison.empty:
            ui.empty_state("Không có sản phẩm phù hợp", "Thay đổi bộ lọc để xem thống kê.")
        else:
            metric = st.selectbox(
                "Chỉ số",
                ["product_count", "rating_sample_size", "avg_rating"],
                format_func=lambda v: {
                    "product_count": "Số sản phẩm",
                    "rating_sample_size": "Số đánh giá có điểm hợp lệ",
                    "avg_rating": "Điểm trung bình",
                }[v],
                key="basic_comparison_metric",
            )
            ui.chart(
                px.bar(
                    comparison,
                    x=dimension,
                    y=metric,
                    labels={dimension: "Nhóm", metric: "Giá trị"},
                    color_discrete_sequence=ui.COLORS,
                )
            )
            st.caption(
                "Điểm của nhóm được tính theo toàn bộ mẫu đánh giá hợp lệ, không lấy trung bình đều giữa các sản phẩm. Số đánh giá không phải số đơn bán."
            )
            ui.table(comparison)
            ui.download(comparison, "so-sanh-co-ban.csv")
    with st.container(border=True):
        st.subheader("Giá danh mục theo tiền tệ")
        if report.catalog_prices.empty:
            ui.empty_state("Chưa có giá và đơn vị hợp lệ", "Không suy đoán hoặc tự quy đổi tiền tệ.")
        else:
            ui.table(report.catalog_prices)
            ui.download(report.catalog_prices, "thong-ke-gia-danh-muc.csv")
    with st.container(border=True):
        st.subheader("Sản phẩm có điểm cao trong mẫu")
        minimum = st.number_input("Số đánh giá hợp lệ tối thiểu", min_value=1, value=3, step=1, key="basic_minimum")
        ranked = report.rankings(int(minimum))
        st.caption(
            "Xếp theo điểm trung bình rồi số mẫu; chỉ phản ánh dữ liệu đã thu, không khẳng định sản phẩm tốt nhất thị trường."
        )
        if ranked.empty:
            ui.empty_state("Chưa đủ mẫu để xếp hạng", "Giảm số mẫu tối thiểu hoặc chờ thêm đánh giá.")
        else:
            ranked = ranked[
                [
                    "product_id",
                    *[c for c in ("product_name", "brand", "category", "currency", "price") if c in ranked],
                    "avg_rating",
                    "rating_sample_size",
                ]
            ]
            ui.table(ranked)
            ui.download(ranked, "xep-hang-trong-mau.csv")
with reviews_tab:
    with st.container(border=True):
        st.subheader("Phân bố điểm đánh giá")
        if not summary["valid_ratings"]:
            ui.empty_state("Chưa có điểm hợp lệ", "Điểm thiếu hoặc ngoài khoảng 1–5 không tham gia thống kê.")
        else:
            ui.chart(
                px.bar(
                    report.star_distribution,
                    x="rating",
                    y="review_count",
                    labels={"rating": "Số sao", "review_count": "Số đánh giá"},
                    color_discrete_sequence=ui.COLORS,
                )
            )
            fractional = report.star_distribution.attrs.get("fractional_ratings", 0)
            if fractional:
                st.caption(
                    f"{fractional:,} điểm lẻ tham gia trung bình nhưng không được làm tròn vào các cột sao nguyên."
                )
            ui.download(report.star_distribution, "phan-bo-diem.csv")
    with st.container(border=True):
        st.subheader("Đánh giá theo ngày")
        daily = report.review_daily
        if daily.empty:
            ui.empty_state("Chưa đủ dữ liệu theo ngày", "Cần điểm và thời gian đánh giá hợp lệ.")
        else:
            plot = daily.copy()
            plot.loc[plot["trend_status"].eq("insufficient_data"), "rolling_avg_rating"] = float("nan")
            ui.chart(
                px.line(
                    plot,
                    x="date",
                    y=["avg_rating", "rolling_avg_rating"],
                    labels={"date": "Ngày (UTC)", "value": "Điểm", "variable": "Chuỗi"},
                    color_discrete_sequence=ui.COLORS,
                )
            )
            st.caption(
                "Trung bình trượt 7 ngày theo số mẫu. Ngày chưa quan sát không được điền 0; đường trung bình ẩn khi chưa đủ mẫu/ngày."
            )
            ui.table(
                daily[
                    [
                        "date",
                        "review_count",
                        "avg_rating",
                        "rolling_avg_rating",
                        "observed_days",
                        "missing_days",
                        "trend_status",
                    ]
                ]
            )
            ui.download(daily, "xu-huong-danh-gia-co-ban.csv")
with price_tab:
    if not report.price_changes.empty:
        with st.container(border=True):
            st.subheader("Bao nhiêu sản phẩm tăng hoặc giảm giá?")
            st.caption("Đếm sản phẩm theo tiền tệ, so giá trung bình ngày đầu với ngày cuối có quan sát trong bộ lọc.")
            movements = report.price_movements()
            ui.table(movements)
            ui.download(movements, "so-san-pham-bien-dong-gia.csv")
    with st.container(border=True):
        st.subheader("Biến động giá trung bình ngày")
        if report.price_daily.empty:
            ui.empty_state("Chưa có lịch sử giá hợp lệ", "Giá danh mục hiện tại không được dùng để tạo lịch sử.")
        else:
            changes = report.price_changes
            available = options(report.price_daily, "product_id")
            product = st.selectbox(
                "Sản phẩm",
                available,
                key="basic_price_product",
                format_func=lambda key: (
                    str(scope.loc[scope["product_id"].astype(str).eq(key), "product_name"].iloc[0])
                    if "product_name" in scope and scope["product_id"].astype(str).eq(key).any()
                    else key
                ),
            )
            history = report.price_daily.loc[report.price_daily["product_id"].astype(str).eq(product)]
            ui.chart(
                px.line(
                    history,
                    x="date",
                    y="avg_price",
                    facet_row="currency",
                    markers=True,
                    labels={"date": "Ngày quan sát (UTC)", "avg_price": "Giá trung bình ngày"},
                    color_discrete_sequence=ui.COLORS,
                ).update_yaxes(matches=None)
            )
            ui.table(
                history[
                    [
                        "date",
                        "currency",
                        "avg_price",
                        *[c for c in ("min_price", "max_price", "data_points") if c in history],
                    ]
                ]
            )
            selected_changes = changes.loc[changes["product_id"].astype(str).eq(product)]
            ui.table(selected_changes)
            st.caption(
                "Biến động (%) = (giá trung bình ngày cuối / ngày đầu − 1) × 100, trong các ngày thực sự quan sát của khoảng chọn. Một ngày giữ trạng thái chưa đủ dữ liệu; không nội suy ngày thiếu hoặc trộn tiền tệ."
            )
            ui.download(selected_changes, "bien-dong-gia-co-ban.csv")
            ui.download(history, "lich-su-gia-san-pham.csv")
with st.expander("Phương pháp và độ bao phủ"):
    st.write(
        "Các chỉ số là thống kê mô tả trên kho đang kết nối và nhóm nguồn đã chọn. Kết quả mô tả mẫu đã thu thập, không đại diện doanh số hoặc toàn bộ thị trường."
    )
    st.write(
        "Tỷ lệ điểm thấp (1–2 sao): "
        + (f"{summary['negative_ratio']:.1%}" if summary["negative_ratio"] is not None else "Chưa có dữ liệu")
    )
    st.write(
        f"{report.coverage['invalid_ratings']:,} điểm thiếu/không hợp lệ trong kết quả; {report.coverage['undated_reviews']:,} đánh giá thiếu ngày trước khi áp dụng bộ lọc thời gian."
    )
    st.write(
        f"{report.coverage['undated_prices']:,} dòng lịch sử giá thiếu ngày trước bộ lọc; các dòng này không tham gia biểu đồ hoặc biến động giá."
    )
    st.write(
        f"{report.coverage['unmatched_reviews']:,} đánh giá không khớp danh mục nguồn; {report.coverage['excluded_price_rows']:,} dòng giá không dùng được trong khoảng chọn; {report.coverage['catalog_price_excluded']:,} giá danh mục thiếu giá/đơn vị hợp lệ."
    )
ui.footer()
