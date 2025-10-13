from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
try:
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import MinMaxScaler
except ImportError:  # pragma: no cover - scikit-learn optional dependency
    PCA = None  # type: ignore[assignment]
    MinMaxScaler = None  # type: ignore[assignment]


class DatasetVisualizationError(Exception):
    """Base class for dataset visualization errors."""


class DatasetNotFoundError(DatasetVisualizationError):
    """Raised when the dataset file cannot be located."""


class DatasetEmptyError(DatasetVisualizationError):
    """Raised when no data is available for the given filters."""


class UnknownVisualizationType(DatasetVisualizationError):
    """Raised when the requested visualization type is not supported."""


def _format_thousands(value: float | int) -> str:
    return f"{int(value):,}".replace(",", ".")


def _categorize_time(hour: int) -> str:
    if 0 <= hour <= 2:
        return "Dini Hari"
    if 3 <= hour <= 5:
        return "Subuh"
    if 6 <= hour <= 9:
        return "Pagi"
    if 10 <= hour <= 13:
        return "Siang"
    if 14 <= hour <= 17:
        return "Sore"
        return "Malam"


BACKGROUND_COLOR = "#111827"
TEXT_COLOR = "#d1d5db"
GRID_COLOR = "#374151"
PRIMARY_COLOR = "#2dd4bf"
SECONDARY_COLOR = "#5eead4"
FILL_COLOR_TRANSPARENT = "rgba(45, 212, 191, 0.2)"
OCEANIC_PALETTE = ["#2dd4bf", "#5eead4", "#a7f3d0", "#67e8f9", "#38bdf8"]
OCEANIC_BAR_PALETTE = ["#0d9488", "#14b8a6", "#2dd4bf", "#5eead4", "#a7f3d0", "#ccfbf1"]

CUSTOM_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        plot_bgcolor=BACKGROUND_COLOR,
        paper_bgcolor=BACKGROUND_COLOR,
        font=dict(color=TEXT_COLOR),
        xaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
        title_font=dict(size=20),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=GRID_COLOR,
            borderwidth=1,
            title_font=dict(color=TEXT_COLOR),
            font=dict(color=TEXT_COLOR),
        ),
    )
)


def _ensure_pca_available() -> None:
    if PCA is None or MinMaxScaler is None:
        raise DatasetVisualizationError(
            "scikit-learn is required for PCA visualizations. "
            "Install it with 'pip install scikit-learn'."
        )


@dataclass
class DatasetVisualizationService:
    dataset_path: Path

    def __post_init__(self) -> None:
        if not self.dataset_path.exists():
            raise DatasetNotFoundError(f"Dataset not found at '{self.dataset_path}'.")
        self._cache_mtime: Optional[float] = None
        self._cached_df: Optional[pd.DataFrame] = None
        self._dispatch: Dict[str, Callable[[pd.DataFrame], Dict[str, Dict[str, str]]]] = {
            "view": self._generate_view_overview,
            "view_distribution": self._generate_view_distribution,
            "view_top_users": self._generate_view_top_users,
            "view_time": self._generate_view_time_distribution,
            "like": self._generate_like_overview,
            "like_distribution": self._generate_like_distribution,
            "like_top_users": self._generate_like_top_users,
            "like_time": self._generate_like_time_distribution,
            "pc": self._generate_pc_overview,
            "pc_distribution": self._generate_pc_distribution,
            "pc_top_users": self._generate_pc_top_users,
            "pc_time": self._generate_pc_time_distribution,
        }

    def generate_html(
        self,
        visualization_type: str,
        created_from: Optional[datetime],
        created_to: Optional[datetime],
    ) -> Dict[str, Dict[str, str]]:
        visualization_type = visualization_type.strip().lower()
        if visualization_type not in self._dispatch:
            supported = ", ".join(sorted(self._dispatch))
            raise UnknownVisualizationType(
                f"Unsupported visualization type '{visualization_type}'. "
                f"Supported types: {supported}"
            )

        df = self._get_filtered_dataframe(created_from, created_to)
        renderer = self._dispatch[visualization_type]
        return renderer(df)

    def _get_filtered_dataframe(
        self,
        created_from: Optional[datetime],
        created_to: Optional[datetime],
    ) -> pd.DataFrame:
        df = self._load_dataframe().copy()
        if created_from and created_to and created_from > created_to:
            raise DatasetVisualizationError(
                "Parameter 'post_created_from' must be earlier than 'post_created_to'."
            )

        if created_from:
            df = df[df["taken_at"] >= pd.Timestamp(created_from)]
        if created_to:
            df = df[df["taken_at"] <= pd.Timestamp(created_to)]

        if df.empty:
            raise DatasetEmptyError("No data found for the provided filters.")
        return df

    def _load_dataframe(self) -> pd.DataFrame:
        current_mtime = self.dataset_path.stat().st_mtime
        if self._cached_df is not None and self._cache_mtime == current_mtime:
            return self._cached_df

        df = pd.read_json(self.dataset_path)
        if "taken_at" not in df:
            raise DatasetVisualizationError("Column 'taken_at' missing from dataset.")

        df["taken_at"] = pd.to_datetime(df["taken_at"], errors="coerce")
        df = df.dropna(subset=["taken_at"])

        # Ensure numeric columns are numeric
        for column in ("view_count", "like_count", "comment_count"):
            if column in df:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        self._cached_df = df
        self._cache_mtime = current_mtime
        return df

    @staticmethod
    def _serialize_figures(
        figures: List[Tuple[str, str, go.Figure]]
    ) -> Dict[str, Dict[str, str]]:
        if not figures:
            raise DatasetVisualizationError("No figures were generated.")

        plots: Dict[str, Dict[str, str]] = {}
        for key, title, fig in figures:
            if key in plots:
                raise DatasetVisualizationError(
                    f"Duplicate plot key '{key}' encountered."
                )
            plots[key] = {
                "title": title,
                "html": fig.to_html(full_html=False, include_plotlyjs="cdn"),
            }
        return plots

    @staticmethod
    def _combine_plots(
        *plot_groups: Optional[Dict[str, Dict[str, str]]]
    ) -> Dict[str, Dict[str, str]]:
        combined: Dict[str, Dict[str, str]] = {}
        valid_groups = [group for group in plot_groups if group]
        if not valid_groups:
            raise DatasetVisualizationError("No plots available to combine.")
        for group in valid_groups:
            for key, value in group.items():
                if key in combined:
                    raise DatasetVisualizationError(
                        f"Duplicate plot key '{key}' encountered while combining plots."
                    )
                combined[key] = value
        if not combined:
            raise DatasetVisualizationError("No plots available to combine.")
        return combined

    def _generate_view_distribution(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        df_view = df[df["view_count"] > 0].copy()
        if df_view.empty:
            raise DatasetEmptyError("Tidak ada data view_count positif untuk visualisasi.")

        df_view["view_count_log1p"] = np.log1p(df_view["view_count"])
        df_view["view_count_formatted"] = df_view["view_count"].apply(_format_thousands)

        df_no_outliers = df_view.copy()
        q1 = df_no_outliers["view_count"].quantile(0.25)
        q3 = df_no_outliers["view_count"].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df_no_outliers = df_no_outliers[
            (df_no_outliers["view_count"] >= lower_bound)
            & (df_no_outliers["view_count"] <= upper_bound)
        ]

        categories = sorted(
            df_view["summary_topic"].dropna().astype(str).unique().tolist()
        )
        if categories:
            df_view["summary_topic"] = pd.Categorical(
                df_view["summary_topic"], categories=categories, ordered=True
            )
            df_no_outliers["summary_topic"] = pd.Categorical(
                df_no_outliers["summary_topic"], categories=categories, ordered=True
            )

        fig_violin = go.Figure()
        fig_violin.add_trace(
            go.Violin(
                x=df_view["summary_topic"] if categories else df_view["summary_topic"],
                y=df_view["view_count_log1p"],
                box_visible=True,
                points="all",
                pointpos=0,
                jitter=0.3,
                marker=dict(size=5, color=SECONDARY_COLOR),
                line=dict(width=1.5, color=PRIMARY_COLOR),
                fillcolor=FILL_COLOR_TRANSPARENT,
                meanline_visible=True,
                hoveron="violins+points",
                text=df_view["username"],
                customdata=df_view[["id", "view_count_formatted"]],
                hovertemplate=(
                    "ID: %{customdata[0]}<br>"
                    "Pengguna: %{text}<br>"
                    "Topik: %{x}<br>"
                    "Log View Count: %{y}<br>"
                    "View Count: %{customdata[1]}<extra></extra>"
                ),
                name="Log View Count",
            )
        )
        fig_violin.update_layout(
            xaxis_title="Topik",
            yaxis_title="Log View Count",
            title="Distribusi Log View Count per Topik",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(
                categoryorder="array",
                categoryarray=categories,
            )
            if categories
            else None,
            showlegend=False,
            height=600,
        )

        mean_view_count = (
            df_no_outliers.groupby("summary_topic", observed=True)["view_count"]
            .mean()
            .reset_index()
        )
        if mean_view_count.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata view_count tanpa outlier."
            )

        mean_view_count["summary_topic"] = pd.Categorical(
            mean_view_count["summary_topic"], categories=categories, ordered=True
        )
        mean_view_count = mean_view_count.sort_values("summary_topic")
        mean_view_count["view_count_formatted"] = mean_view_count["view_count"].apply(
            _format_thousands
        )

        fig_bar = go.Figure()
        fig_bar.add_trace(
            go.Bar(
                x=mean_view_count["summary_topic"].astype(str),
                y=mean_view_count["view_count"],
                marker_color=PRIMARY_COLOR,
                marker_line_color=SECONDARY_COLOR,
                marker_line_width=1.5,
                text=mean_view_count["view_count_formatted"],
                textposition="auto",
                hovertemplate=(
                    "Topik: %{x}<br>Mean View Count: %{text}<extra></extra>"
                ),
            )
        )
        fig_bar.update_layout(
            xaxis_title="Topik",
            yaxis_title="Mean View Count",
            title="Rata-rata View Count per Topik (Tanpa Outlier)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(
                categoryorder="array",
                categoryarray=categories,
            )
            if categories
            else None,
            showlegend=False,
            height=600,
        )
        fig_bar.update_traces(textfont_color=BACKGROUND_COLOR, textfont_size=12)

        return self._serialize_figures(
            [
                (
                    "view_distribution_violin",
                    "Distribusi Log View Count per Topik",
                    fig_violin,
                ),
                (
                    "view_distribution_mean_bar",
                    "Rata-rata View Count per Topik (Tanpa Outlier)",
                    fig_bar,
                ),
            ]
        )

    def _generate_view_overview(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        plots = [
            self._generate_view_distribution(df),
            self._generate_view_top_users(df),
            self._generate_view_time_distribution(df),
        ]
        return self._combine_plots(*plots)

    def _generate_view_top_users(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        df_view = df[df["view_count"] > 0].copy()
        if df_view.empty:
            raise DatasetEmptyError("Tidak ada data view_count positif untuk visualisasi.")

        mean_views = (
            df_view.groupby(["summary_topic", "username"], observed=True)["view_count"]
            .mean()
            .reset_index()
        )

        if mean_views.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata view_count per pengguna."
            )

        top_5_per_topic = (
            mean_views.sort_values("view_count", ascending=False)
            .groupby("summary_topic", group_keys=False, observed=True)
            .head(5)
            .reset_index(drop=True)
        )

        if top_5_per_topic.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menentukan Top 5 view_count per topik."
            )

        df_top_5 = df_view.merge(
            top_5_per_topic[["summary_topic", "username"]],
            on=["summary_topic", "username"],
        )

        if df_top_5.empty:
            raise DatasetEmptyError(
                "Data tidak cukup setelah penggabungan Top 5 per topik."
            )

        df_top_5["view_count_log1p"] = np.log1p(df_top_5["view_count"])

        categories = sorted(
            df_top_5["summary_topic"].dropna().astype(str).unique().tolist()
        )

        fig = px.strip(
            df_top_5,
            x="summary_topic",
            y="view_count_log1p",
            color="username",
            color_discrete_sequence=OCEANIC_PALETTE,
            hover_data=["username", "id", "view_count"],
            title="Distribusi Log View Count untuk Top 5 Pengguna per Topik",
            labels={
                "summary_topic": "Topik",
                "view_count_log1p": "Log View Count",
                "username": "Pengguna",
                "view_count": "View Count Asli",
            },
            category_orders={"summary_topic": categories} if categories else None,
        )

        fig.update_layout(
            xaxis_title="Topik",
            yaxis_title="Log View Count",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            legend_title_text="Pengguna",
            height=600,
            showlegend=True,
        )

        return self._serialize_figures(
            [
                (
                    "view_top_users_strip",
                    "Distribusi Log View Count untuk Top 5 Pengguna per Topik",
                    fig,
                )
            ]
        )

    def _generate_view_time_distribution(
        self, df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
        df_view = df[df["view_count"] > 0].copy()
        if df_view.empty:
            raise DatasetEmptyError("Tidak ada data view_count positif untuk visualisasi.")

        df_view["taken_at"] = pd.to_datetime(df_view["taken_at"])
        df_view["day"] = df_view["taken_at"].dt.day_name()
        df_view["hour"] = df_view["taken_at"].dt.hour
        df_view["waktu_post_manual"] = df_view["hour"].apply(_categorize_time)

        day_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        waktu_order = [
            "Dini Hari",
            "Subuh",
            "Pagi",
            "Siang",
            "Sore",
            "Malam",
        ]

        df_view["day"] = pd.Categorical(df_view["day"], categories=day_order, ordered=True)
        df_view["waktu_post_manual"] = pd.Categorical(
            df_view["waktu_post_manual"], categories=waktu_order, ordered=True
        )

        def remove_outliers(group: pd.DataFrame) -> pd.DataFrame:
            q1 = group["view_count"].quantile(0.25)
            q3 = group["view_count"].quantile(0.75)
            iqr = q3 - q1
            lower_bound = max(0, q1 - 1.5 * iqr)
            upper_bound = q3 + 1.5 * iqr
            return group[
                (group["view_count"] >= lower_bound)
                & (group["view_count"] <= upper_bound)
            ]

        df_no_outliers = (
            df_view.groupby("day", group_keys=False, observed=True)
            .apply(remove_outliers)
            .reset_index(drop=True)
        )

        if df_no_outliers.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung view_count tanpa outlier per hari."
            )

        df_no_outliers["day"] = pd.Categorical(
            df_no_outliers["day"], categories=day_order, ordered=True
        )
        df_no_outliers["waktu_post_manual"] = pd.Categorical(
            df_no_outliers["waktu_post_manual"], categories=waktu_order, ordered=True
        )

        mean_view_count = (
            df_no_outliers.groupby(["day", "waktu_post_manual"], observed=True)["view_count"]
            .mean()
            .reset_index()
        )

        if mean_view_count.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata view_count per hari dan waktu."
            )

        mean_view_count["view_count_formatted"] = mean_view_count["view_count"].apply(
            _format_thousands
        )

        pivot_data = (
            mean_view_count.pivot(
                index="day", columns="waktu_post_manual", values="view_count"
            ).fillna(0)
        )
        pivot_formatted = (
            mean_view_count.pivot(
                index="day",
                columns="waktu_post_manual",
                values="view_count_formatted",
            ).fillna("0")
        )

        fig = go.Figure()
        for idx, waktu in enumerate(waktu_order):
            if waktu not in pivot_data.columns:
                continue
            fig.add_trace(
                go.Bar(
                    x=pivot_data.index.tolist(),
                    y=pivot_data[waktu].tolist(),
                    name=waktu,
                    marker_color=OCEANIC_BAR_PALETTE[idx],
                    text=pivot_formatted[waktu].tolist(),
                    textposition="inside",
                    hovertemplate=(
                        "Hari: %{x}<br>"
                        f"Waktu: {waktu}<br>"
                        "Mean View Count: %{text}<extra></extra>"
                    ),
                )
            )

        fig.update_layout(
            xaxis_title="Hari",
            yaxis_title="Mean View Count",
            title="Rata-rata View Count per Hari dan Waktu Upload (Tanpa Outlier)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(categoryorder="array", categoryarray=day_order),
            barmode="stack",
            showlegend=True,
            height=600,
        )
        fig.update_layout(legend_title_text="Waktu Upload")
        fig.update_traces(textfont_color="black", textfont_size=12)

        return self._serialize_figures(
            [
                (
                    "view_time_stacked_bar",
                    "Rata-rata View Count per Hari dan Waktu Upload (Tanpa Outlier)",
                    fig,
                )
            ]
        )

    def _generate_like_distribution(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        df_like = df[(df["view_count"] > 0) & (df["like_count"] >= 0)].copy()
        if df_like.empty:
            raise DatasetEmptyError(
                "Tidak ada data dengan view_count positif untuk menghitung like_percentage."
            )

        df_like["like_percentage"] = (
            df_like["like_count"] / df_like["view_count"] * 100
        )
        df_like["like_percentage_formatted"] = df_like["like_percentage"].apply(
            lambda x: f"{x:.2f}%"
        )

        df_no_outliers = df_like.copy()
        q1 = df_no_outliers["like_percentage"].quantile(0.25)
        q3 = df_no_outliers["like_percentage"].quantile(0.75)
        iqr = q3 - q1
        lower_bound = max(0, q1 - 1.5 * iqr)
        upper_bound = q3 + 1.5 * iqr
        df_no_outliers = df_no_outliers[
            (df_no_outliers["like_percentage"] >= lower_bound)
            & (df_no_outliers["like_percentage"] <= upper_bound)
        ]

        categories = sorted(
            df_like["summary_topic"].dropna().astype(str).unique().tolist()
        )
        if categories:
            df_like["summary_topic"] = pd.Categorical(
                df_like["summary_topic"], categories=categories, ordered=True
            )
            df_no_outliers["summary_topic"] = pd.Categorical(
                df_no_outliers["summary_topic"], categories=categories, ordered=True
            )

        fig_violin = go.Figure()
        fig_violin.add_trace(
            go.Violin(
                x=df_like["summary_topic"] if categories else df_like["summary_topic"],
                y=df_like["like_percentage"],
                box_visible=True,
                points="all",
                pointpos=0,
                jitter=0.3,
                marker=dict(size=5, color=SECONDARY_COLOR),
                line=dict(width=1.5, color=PRIMARY_COLOR),
                fillcolor=FILL_COLOR_TRANSPARENT,
                meanline_visible=True,
                hoveron="violins+points",
                text=df_like["username"],
                customdata=df_like[["id", "like_percentage_formatted"]],
                hovertemplate=(
                    "ID: %{customdata[0]}<br>"
                    "Pengguna: %{text}<br>"
                    "Topik: %{x}<br>"
                    "Like Percentage: %{customdata[1]}<extra></extra>"
                ),
                name="Like Percentage",
            )
        )

        fig_violin.update_layout(
            xaxis_title="Topik",
            yaxis_title="Like Percentage (%)",
            title="Distribusi Like Percentage per Topik",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(
                categoryorder="array",
                categoryarray=categories,
            )
            if categories
            else None,
            showlegend=False,
            height=600,
        )

        mean_like_percentage = (
            df_no_outliers.groupby("summary_topic", observed=True)["like_percentage"]
            .mean()
            .reset_index()
        )

        if mean_like_percentage.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata like_percentage per topik."
            )

        mean_like_percentage["summary_topic"] = pd.Categorical(
            mean_like_percentage["summary_topic"], categories=categories, ordered=True
        )
        mean_like_percentage = mean_like_percentage.sort_values("summary_topic")
        mean_like_percentage["like_percentage_formatted"] = mean_like_percentage[
            "like_percentage"
        ].apply(lambda x: f"{x:.2f}%")

        fig_bar = go.Figure()
        fig_bar.add_trace(
            go.Bar(
                x=mean_like_percentage["summary_topic"].astype(str),
                y=mean_like_percentage["like_percentage"],
                marker_color=PRIMARY_COLOR,
                marker_line_color=SECONDARY_COLOR,
                marker_line_width=1.5,
                text=mean_like_percentage["like_percentage_formatted"],
                textposition="auto",
                hovertemplate=(
                    "Topik: %{x}<br>Mean Like Percentage: %{text}<extra></extra>"
                ),
            )
        )

        fig_bar.update_layout(
            xaxis_title="Topik",
            yaxis_title="Mean Like Percentage (%)",
            title="Rata-rata Like Percentage per Topik (Tanpa Outlier)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(
                categoryorder="array",
                categoryarray=categories,
            )
            if categories
            else None,
            showlegend=False,
            height=600,
        )
        fig_bar.update_traces(textfont_color=BACKGROUND_COLOR, textfont_size=12)

        return self._serialize_figures(
            [
                (
                    "like_distribution_violin",
                    "Distribusi Like Percentage per Topik",
                    fig_violin,
                ),
                (
                    "like_distribution_mean_bar",
                    "Rata-rata Like Percentage per Topik (Tanpa Outlier)",
                    fig_bar,
                ),
            ]
        )

    def _generate_like_top_users(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        df_like = df[(df["view_count"] > 0) & (df["like_count"] >= 0)].copy()
        if df_like.empty:
            raise DatasetEmptyError(
                "Tidak ada data dengan view_count positif untuk menghitung like_percentage."
            )

        mean_views = (
            df_like.groupby(["summary_topic", "username"], observed=True)["view_count"]
            .mean()
            .reset_index()
        )

        if mean_views.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata view_count per pengguna."
            )

        top_5_per_topic = (
            mean_views.sort_values("view_count", ascending=False)
            .groupby("summary_topic", group_keys=False, observed=True)
            .head(5)
            .reset_index(drop=True)
        )

        if top_5_per_topic.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menentukan Top 5 pengguna per topik."
            )

        df_top_5 = df_like.merge(
            top_5_per_topic[["summary_topic", "username"]],
            on=["summary_topic", "username"],
        )

        if df_top_5.empty:
            raise DatasetEmptyError(
                "Data tidak cukup setelah penggabungan Top 5 per topik."
            )

        df_top_5["like_percentage"] = (
            df_top_5["like_count"] / df_top_5["view_count"] * 100
        )

        categories = sorted(
            df_top_5["summary_topic"].dropna().astype(str).unique().tolist()
        )

        fig = px.strip(
            df_top_5,
            x="summary_topic",
            y="like_percentage",
            color="username",
            color_discrete_sequence=OCEANIC_PALETTE,
            hover_data=["username", "id", "like_percentage"],
            title="Distribusi Like Percentage untuk Top 5 Pengguna per Topik",
            labels={
                "summary_topic": "Topik",
                "like_percentage": "Like Percentage (%)",
                "username": "Pengguna",
            },
            category_orders={"summary_topic": categories} if categories else None,
        )

        fig.update_layout(
            xaxis_title="Topik",
            yaxis_title="Like Percentage (%)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            legend_title_text="Pengguna",
            height=600,
            showlegend=True,
        )

        return self._serialize_figures(
            [
                (
                    "like_top_users_strip",
                    "Distribusi Like Percentage untuk Top 5 Pengguna per Topik",
                    fig,
                )
            ]
        )

    def _generate_like_time_distribution(
        self, df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
        df_like = df[(df["view_count"] > 0) & (df["like_count"] >= 0)].copy()
        if df_like.empty:
            raise DatasetEmptyError(
                "Tidak ada data dengan view_count positif untuk menghitung like_percentage."
            )

        df_like["like_percentage"] = (
            df_like["like_count"] / df_like["view_count"] * 100
        )

        df_like["taken_at"] = pd.to_datetime(df_like["taken_at"])
        df_like["day"] = df_like["taken_at"].dt.day_name()
        df_like["hour"] = df_like["taken_at"].dt.hour
        df_like["waktu_post_manual"] = df_like["hour"].apply(_categorize_time)

        day_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        waktu_order = [
            "Dini Hari",
            "Subuh",
            "Pagi",
            "Siang",
            "Sore",
            "Malam",
        ]

        df_like["day"] = pd.Categorical(df_like["day"], categories=day_order, ordered=True)
        df_like["waktu_post_manual"] = pd.Categorical(
            df_like["waktu_post_manual"], categories=waktu_order, ordered=True
        )

        def remove_outliers(group: pd.DataFrame) -> pd.DataFrame:
            q1 = group["like_percentage"].quantile(0.25)
            q3 = group["like_percentage"].quantile(0.75)
            iqr = q3 - q1
            lower_bound = max(0, q1 - 1.5 * iqr)
            upper_bound = q3 + 1.5 * iqr
            return group[
                (group["like_percentage"] >= lower_bound)
                & (group["like_percentage"] <= upper_bound)
            ]

        df_no_outliers = (
            df_like.groupby("day", group_keys=False, observed=True)
            .apply(remove_outliers)
            .reset_index(drop=True)
        )

        if df_no_outliers.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung like_percentage tanpa outlier."
            )

        df_no_outliers["day"] = pd.Categorical(
            df_no_outliers["day"], categories=day_order, ordered=True
        )
        df_no_outliers["waktu_post_manual"] = pd.Categorical(
            df_no_outliers["waktu_post_manual"], categories=waktu_order, ordered=True
        )

        mean_like_percentage = (
            df_no_outliers.groupby(["day", "waktu_post_manual"], observed=True)[
                "like_percentage"
            ]
            .mean()
            .reset_index()
        )

        if mean_like_percentage.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata like_percentage per hari dan waktu."
            )

        mean_like_percentage["like_percentage_formatted"] = mean_like_percentage[
            "like_percentage"
        ].apply(lambda x: f"{x:.2f}%")

        pivot_data = (
            mean_like_percentage.pivot(
                index="day",
                columns="waktu_post_manual",
                values="like_percentage",
            ).fillna(0)
        )
        pivot_formatted = (
            mean_like_percentage.pivot(
                index="day",
                columns="waktu_post_manual",
                values="like_percentage_formatted",
            ).fillna("0%")
        )

        fig = go.Figure()
        for idx, waktu in enumerate(waktu_order):
            if waktu not in pivot_data.columns:
                continue
            fig.add_trace(
                go.Bar(
                    x=pivot_data.index.tolist(),
                    y=pivot_data[waktu].tolist(),
                    name=waktu,
                    marker_color=OCEANIC_BAR_PALETTE[idx],
                    text=pivot_formatted[waktu].tolist(),
                    textposition="inside",
                    hovertemplate=(
                        "Hari: %{x}<br>"
                        f"Waktu: {waktu}<br>"
                        "Mean Like Percentage: %{text}<extra></extra>"
                    ),
                )
            )

        fig.update_layout(
            xaxis_title="Hari",
            yaxis_title="Mean Like Percentage (%)",
            title="Rata-rata Like Percentage per Hari dan Waktu Upload (Tanpa Outlier)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(categoryorder="array", categoryarray=day_order),
            barmode="stack",
            showlegend=True,
            height=600,
        )
        fig.update_layout(legend_title_text="Waktu Upload")
        fig.update_traces(textfont_color="black", textfont_size=12, textangle=0)

        return self._serialize_figures(
            [
                (
                    "like_time_stacked_bar",
                    "Rata-rata Like Percentage per Hari dan Waktu Upload (Tanpa Outlier)",
                    fig,
                )
            ]
        )

    def _generate_like_overview(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        plots = [
            self._generate_like_distribution(df),
            self._generate_like_top_users(df),
            self._generate_like_time_distribution(df),
        ]
        return self._combine_plots(*plots)

    def _prepare_pc_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        _ensure_pca_available()

        df_pc = df[df["view_count"] > 0].copy()
        if df_pc.empty:
            raise DatasetEmptyError(
                "Tidak ada data view_count positif untuk menghitung skor PCA."
            )

        features = ["view_count", "like_count"]
        valid = df_pc[features].dropna()
        if valid.empty:
            raise DatasetEmptyError(
                "Data tidak memiliki nilai view_count dan like_count yang lengkap untuk PCA."
            )

        pca = PCA(n_components=1)
        pc_values = pca.fit_transform(valid[features]).flatten()

        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(pc_values.reshape(-1, 1)).flatten() * 100

        df_pc = df_pc.loc[valid.index].copy()
        df_pc["PC1_scaled"] = scaled
        df_pc["PC1_scaled_log1p"] = np.log1p(df_pc["PC1_scaled"])
        df_pc["PC1_formatted"] = df_pc["PC1_scaled"].apply(lambda x: f"{x:.2f}")
        df_pc["view_count_formatted"] = df_pc["view_count"].apply(_format_thousands)

        if df_pc["PC1_scaled"].isna().all():
            raise DatasetEmptyError(
                "Tidak ada skor PCA yang valid setelah proses normalisasi."
            )

        return df_pc

    def _generate_pc_distribution(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        df_pc = self._prepare_pc_dataframe(df)

        categories = sorted(
            df_pc["summary_topic"].dropna().astype(str).unique().tolist()
        )
        if categories:
            df_pc["summary_topic"] = pd.Categorical(
                df_pc["summary_topic"], categories=categories, ordered=True
            )

        fig_violin = go.Figure()
        fig_violin.add_trace(
            go.Violin(
                x=df_pc["summary_topic"] if categories else df_pc["summary_topic"],
                y=df_pc["PC1_scaled_log1p"],
                box_visible=True,
                points="all",
                pointpos=0,
                jitter=0.3,
                marker=dict(size=5, color=SECONDARY_COLOR),
                line=dict(width=1.5, color=PRIMARY_COLOR),
                fillcolor=FILL_COLOR_TRANSPARENT,
                meanline_visible=True,
                hoveron="violins+points",
                text=df_pc["username"],
                customdata=df_pc[["id", "PC1_formatted"]],
                hovertemplate=(
                    "ID: %{customdata[0]}<br>"
                    "Pengguna: %{text}<br>"
                    "Topik: %{x}<br>"
                    "Log PC1 Scaled: %{y:.2f}<br>"
                    "PC1 Scaled (0-100): %{customdata[1]}<extra></extra>"
                ),
                name="Log PC1 Scaled",
            )
        )

        fig_violin.update_layout(
            xaxis_title="Topik",
            yaxis_title="Log PC1 Scaled (View & Like)",
            title="Distribusi Log PC1 Scaled per Topik",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(
                categoryorder="array",
                categoryarray=categories,
            )
            if categories
            else None,
            showlegend=False,
            height=600,
        )

        df_bar = df_pc.dropna(subset=["PC1_scaled"]).copy()
        if df_bar.empty:
            raise DatasetEmptyError(
                "Tidak ada data PC1_scaled yang valid untuk visualisasi PC."
            )

        q1 = df_bar["PC1_scaled"].quantile(0.25)
        q3 = df_bar["PC1_scaled"].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df_bar_no_outliers = df_bar[
            (df_bar["PC1_scaled"] >= lower_bound)
            & (df_bar["PC1_scaled"] <= upper_bound)
        ]

        mean_pc1 = (
            df_bar_no_outliers.groupby("summary_topic", observed=True)["PC1_scaled"]
            .mean()
            .reset_index()
        )
        if mean_pc1.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata PC1 per topik."
            )

        if categories:
            mean_pc1["summary_topic"] = pd.Categorical(
                mean_pc1["summary_topic"], categories=categories, ordered=True
            )
            mean_pc1 = mean_pc1.sort_values("summary_topic")
        mean_pc1["mean_pc1_formatted"] = mean_pc1["PC1_scaled"].apply(
            lambda x: f"{x:.2f}"
        )

        fig_bar = go.Figure()
        fig_bar.add_trace(
            go.Bar(
                x=mean_pc1["summary_topic"].astype(str),
                y=mean_pc1["PC1_scaled"],
                marker_color=PRIMARY_COLOR,
                marker_line_color=SECONDARY_COLOR,
                marker_line_width=1.5,
                text=mean_pc1["mean_pc1_formatted"],
                textposition="auto",
                hovertemplate=(
                    "Topik: %{x}<br>Mean PC1 Scaled: %{text}<extra></extra>"
                ),
            )
        )

        fig_bar.update_layout(
            xaxis_title="Topik",
            yaxis_title="Mean PC1 Scaled (0-100)",
            title="Rata-rata PC1 Scaled per Topik (Tanpa Outlier)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(
                categoryorder="array",
                categoryarray=categories,
            )
            if categories
            else None,
            showlegend=False,
            height=600,
        )
        fig_bar.update_traces(textfont_color=BACKGROUND_COLOR, textfont_size=12)

        return self._serialize_figures(
            [
                (
                    "pc_distribution_violin",
                    "Distribusi Log PC1 Scaled per Topik",
                    fig_violin,
                ),
                (
                    "pc_distribution_mean_bar",
                    "Rata-rata PC1 Scaled per Topik (Tanpa Outlier)",
                    fig_bar,
                ),
            ]
        )

    def _generate_pc_top_users(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        df_pc = self._prepare_pc_dataframe(df)

        mean_score = (
            df_pc.groupby(["summary_topic", "username"], observed=True)["PC1_scaled"]
            .mean()
            .reset_index()
        )
        if mean_score.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung skor PCA per pengguna."
            )

        top_5_per_topic = (
            mean_score.sort_values("PC1_scaled", ascending=False)
            .groupby("summary_topic", group_keys=False, observed=True)
            .head(5)
            .reset_index(drop=True)
        )

        if top_5_per_topic.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menentukan Top 5 skor PCA per topik."
            )

        df_top_5 = df_pc.merge(
            top_5_per_topic[["summary_topic", "username"]],
            on=["summary_topic", "username"],
        )

        if df_top_5.empty:
            raise DatasetEmptyError(
                "Data tidak cukup setelah penggabungan Top 5 skor PCA per topik."
            )

        df_top_5["PC1_scaled_log1p"] = np.log1p(df_top_5["PC1_scaled"])
        df_top_5["PC1_formatted"] = df_top_5["PC1_scaled"].apply(
            lambda x: f"{x:.2f}"
        )

        categories = sorted(
            df_top_5["summary_topic"].dropna().astype(str).unique().tolist()
        )

        fig = px.strip(
            df_top_5,
            x="summary_topic",
            y="PC1_scaled_log1p",
            color="username",
            color_discrete_sequence=OCEANIC_PALETTE,
            custom_data=["username", "id", "PC1_formatted"],
            title="Distribusi Skor PCA (Log) untuk Top 5 Pengguna per Topik",
            labels={
                "summary_topic": "Topik",
                "PC1_scaled_log1p": "Skor Kinerja (Log)",
                "username": "Pengguna",
            },
            category_orders={"summary_topic": categories} if categories else None,
        )

        fig.update_traces(
            hovertemplate=(
                "Pengguna: %{customdata[0]}<br>"
                "ID: %{customdata[1]}<br>"
                "Skor (Log): %{y:.2f}<br>"
                "Skor Asli (0-100): %{customdata[2]}<extra></extra>"
            )
        )

        fig.update_layout(
            xaxis_title="Topik",
            yaxis_title="Skor Kinerja (Log)",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            legend_title_text="Pengguna",
            height=600,
            showlegend=True,
        )

        return self._serialize_figures(
            [
                (
                    "pc_top_users_strip",
                    "Distribusi Skor PCA (Log) untuk Top 5 Pengguna per Topik",
                    fig,
                )
            ]
        )

    def _generate_pc_time_distribution(
        self, df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
        df_pc = self._prepare_pc_dataframe(df)

        df_pc["taken_at"] = pd.to_datetime(df_pc["taken_at"])
        df_pc["day"] = df_pc["taken_at"].dt.day_name()
        df_pc["hour"] = df_pc["taken_at"].dt.hour
        df_pc["waktu_post_manual"] = df_pc["hour"].apply(_categorize_time)

        day_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        waktu_order = [
            "Dini Hari",
            "Subuh",
            "Pagi",
            "Siang",
            "Sore",
            "Malam",
        ]

        df_pc["day"] = pd.Categorical(df_pc["day"], categories=day_order, ordered=True)
        df_pc["waktu_post_manual"] = pd.Categorical(
            df_pc["waktu_post_manual"], categories=waktu_order, ordered=True
        )

        def remove_outliers(group: pd.DataFrame) -> pd.DataFrame:
            q1 = group["PC1_scaled"].quantile(0.25)
            q3 = group["PC1_scaled"].quantile(0.75)
            iqr = q3 - q1
            lower_bound = max(0, q1 - 1.5 * iqr)
            upper_bound = q3 + 1.5 * iqr
            return group[
                (group["PC1_scaled"] >= lower_bound)
                & (group["PC1_scaled"] <= upper_bound)
            ]

        df_no_outliers = (
            df_pc.dropna(subset=["PC1_scaled"])
            .groupby("day", group_keys=False, observed=True)
            .apply(remove_outliers)
            .reset_index(drop=True)
        )

        if df_no_outliers.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung skor PCA tanpa outlier per hari."
            )

        df_no_outliers["day"] = pd.Categorical(
            df_no_outliers["day"], categories=day_order, ordered=True
        )
        df_no_outliers["waktu_post_manual"] = pd.Categorical(
            df_no_outliers["waktu_post_manual"], categories=waktu_order, ordered=True
        )

        mean_pc1_scaled = (
            df_no_outliers.groupby(["day", "waktu_post_manual"], observed=True)[
                "PC1_scaled"
            ]
            .mean()
            .reset_index()
        )

        if mean_pc1_scaled.empty:
            raise DatasetEmptyError(
                "Data tidak cukup untuk menghitung rata-rata skor PCA per hari dan waktu."
            )

        mean_pc1_scaled["pc1_formatted"] = mean_pc1_scaled["PC1_scaled"].apply(
            lambda x: f"{x:.2f}"
        )

        pivot_data = (
            mean_pc1_scaled.pivot(
                index="day", columns="waktu_post_manual", values="PC1_scaled"
            ).fillna(0)
        )
        pivot_formatted = (
            mean_pc1_scaled.pivot(
                index="day", columns="waktu_post_manual", values="pc1_formatted"
            ).fillna("0.00")
        )

        fig = go.Figure()
        for idx, waktu in enumerate(waktu_order):
            if waktu not in pivot_data.columns:
                continue
            fig.add_trace(
                go.Bar(
                    x=pivot_data.index.tolist(),
                    y=pivot_data[waktu].tolist(),
                    name=waktu,
                    marker_color=OCEANIC_BAR_PALETTE[idx],
                    text=pivot_formatted[waktu].tolist(),
                    textposition="inside",
                    hovertemplate=(
                        "Hari: %{x}<br>"
                        f"Waktu: {waktu}<br>"
                        "Mean Skor Kinerja: %{text}<extra></extra>"
                    ),
                )
            )

        fig.update_layout(
            xaxis_title="Hari",
            yaxis_title="Mean Skor Kinerja (PC1 Scaled)",
            title="Rata-rata Skor Kinerja per Hari dan Waktu Upload",
            template=CUSTOM_TEMPLATE,
            xaxis_tickangle=45,
            xaxis=dict(categoryorder="array", categoryarray=day_order),
            barmode="stack",
            showlegend=True,
            height=600,
        )
        fig.update_layout(legend_title_text="Waktu Upload")
        fig.update_traces(textfont_color="black", textfont_size=12)

        return self._serialize_figures(
            [
                (
                    "pc_time_stacked_bar",
                    "Rata-rata Skor Kinerja per Hari dan Waktu Upload",
                    fig,
                )
            ]
        )

    def _generate_pc_overview(self, df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        plots = [
            self._generate_pc_distribution(df),
            self._generate_pc_top_users(df),
            self._generate_pc_time_distribution(df),
        ]
        return self._combine_plots(*plots)

    def generate_table_data(
        self,
        created_from: Optional[datetime],
        created_to: Optional[datetime],
    ) -> List[Dict[str, float | int | str | None]]:
        df = self._get_filtered_dataframe(created_from, created_to)
        _ensure_pca_available()

        df_table = df[df["view_count"] > 0].copy()
        if df_table.empty:
            raise DatasetEmptyError(
                "Tidak ada data view_count positif untuk membangun tabel."
            )

        features = ["view_count", "like_count"]
        valid = df_table[features].dropna()
        if valid.empty:
            raise DatasetEmptyError(
                "Data tidak memiliki nilai view_count dan like_count yang lengkap untuk PCA."
            )

        pca = PCA(n_components=1)
        pc_values = pca.fit_transform(valid[features]).flatten()

        scaler = MinMaxScaler()
        scaled_scores = scaler.fit_transform(pc_values.reshape(-1, 1)).flatten() * 100
        df_table.loc[valid.index, "PC1_scaled"] = scaled_scores

        df_table["like_percentage"] = (
            df_table["like_count"] / df_table["view_count"] * 100
        )

        table = df_table.loc[
            valid.index,
            ["id", "summary_title", "view_count", "like_percentage", "PC1_scaled"],
        ].copy()

        table = table.rename(
            columns={
                "summary_title": "summary_judul",
                "view_count": "view",
                "like_percentage": "persentase_like",
                "PC1_scaled": "pc1_scaled",
            }
        )

        table["persentase_like"] = table["persentase_like"].round(2)
        table["pc1_scaled"] = table["pc1_scaled"].round(2)

        table = table.sort_values("pc1_scaled", ascending=False)

        return table.to_dict(orient="records")
