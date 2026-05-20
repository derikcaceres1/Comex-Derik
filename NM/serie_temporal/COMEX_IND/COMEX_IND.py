"""COMEX India (IND) — Nova Metodologia v2."""

import os
import sys
from datetime import datetime

os.environ.setdefault("COSTDRIVERS_PASSWORD", "not-used-in-developing-mode")
os.environ.setdefault("COSTDRIVERS_ENDPOINT", "https://api-costdrivers.gep.com/costdrivers-api")
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

_PIPELINE_ROOT = Path(__file__).resolve().parent
_PROJECT_ROOT = _PIPELINE_ROOT.parent.parent.parent
_SERIE_TEMPORAL = _PROJECT_ROOT / "NM" / "serie_temporal"
if str(_SERIE_TEMPORAL) not in sys.path:
    sys.path.insert(0, str(_SERIE_TEMPORAL))

from costdrivers_comex_NM_v2 import ComexPipelineNMv2
from nm_config import NMConfig

_SCRAPER_PARQUET_DIRNAME = "scraper_parquet"
_PORTAL_REFRESH_DAYS = 90


class COMEX_IND_NM_v2(ComexPipelineNMv2):
    """Pipeline COMEX India — Nova Metodologia v2."""

    def __init__(
        self,
        config: Optional[NMConfig] = None,
        start_date: Optional[datetime] = None,
        use_azure: bool = True,
        developing: bool = False,
        run_scraper: bool = True,
    ) -> None:
        """Initialize the India COMEX pipeline with optional config, date range, and scraper settings."""
        super().__init__(
            iso_code="IND",
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )
        self.run_scraper = run_scraper

    def _get_country_name(self) -> str:
        """Return the country name used for filtering: 'Índia'."""
        return "Índia"

    def _scraper_parquet_dir(self) -> Path:
        """Resolve the scraper parquet directory, creating it if it does not exist."""
        candidates: List[Path] = []
        if self.developing:
            candidates.append(Path("NM") / "dados" / self.iso_code / _SCRAPER_PARQUET_DIRNAME)
            candidates.append(Path("dados") / self.iso_code / _SCRAPER_PARQUET_DIRNAME)
        candidates.append(_PROJECT_ROOT / "NM" / "dados" / self.iso_code / _SCRAPER_PARQUET_DIRNAME)
        for cand in candidates:
            resolved = cand if cand.is_absolute() else Path.cwd() / cand
            if resolved.exists():
                return resolved
        fallback = _PROJECT_ROOT / "NM" / "dados" / self.iso_code / _SCRAPER_PARQUET_DIRNAME
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

    def _list_scraped_months(self, base_dir: Path) -> List[Tuple[int, int]]:
        """Return a sorted list of (year, month) tuples for all months with parquet data in base_dir."""
        months: List[Tuple[int, int]] = []
        if not base_dir.exists():
            return months
        for year_dir in sorted(base_dir.glob("year=*")):
            try:
                year = int(year_dir.name.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
            for month_dir in sorted(year_dir.glob("month=*")):
                try:
                    month = int(month_dir.name.split("=", 1)[1])
                except (ValueError, IndexError):
                    continue
                if any(month_dir.glob("*.parquet")):
                    months.append((year, month))
        return sorted(set(months))

    def _maybe_scrape_next_month(self, base_dir: Path) -> None:
        """Trigger scraper for the next month if data is stale and the target date is not in the future."""
        if not self.run_scraper:
            self.logger.info("run_scraper=False — pulando scrape.")
            return
        scraped = self._list_scraped_months(base_dir)
        if not scraped:
            self.logger.warning("Nenhum mês escrapeado encontrado em %s", base_dir)
            return
        latest_year, latest_month = scraped[-1]
        latest_date = datetime(latest_year, latest_month, 1)
        if (datetime.now() - latest_date).days < _PORTAL_REFRESH_DAYS:
            self.logger.info(
                "Último mês escrapeado %04d-%02d ainda recente — não tentando novo scrape.",
                latest_year, latest_month,
            )
            return

        next_month = latest_month + 1
        next_year = latest_year
        if next_month > 12:
            next_month = 1
            next_year += 1

        next_date = datetime(next_year, next_month, 1)
        if next_date > datetime.now():
            self.logger.info("Próximo mês %04d-%02d ainda no futuro — pulando.", next_year, next_month)
            return

        self.logger.info("Tentando escrapear mês adicional: %04d-%02d", next_year, next_month)
        try:
            from COMEX_IND.scraper import orchestrator
            orchestrator.run_range([(next_year, next_month)])
        except Exception as exc:
            self.logger.warning("Scrape do mês %04d-%02d falhou: %s", next_year, next_month, exc)

    def _read_scraped_dataset(self, base_dir: Path, flow: str) -> pd.DataFrame:
        """Read and combine parquet files, filtering by trade flow and pipeline start date."""
        if not base_dir.exists():
            self.logger.warning("Diretório de parquets não existe: %s", base_dir)
            return pd.DataFrame()

        start_year = self.start_date.year
        start_month = self.start_date.month

        parquet_files: List[str] = []
        for year, month in self._list_scraped_months(base_dir):
            if (year, month) < (start_year, start_month):
                continue
            month_dir = base_dir / f"year={year}" / f"month={month:02d}"
            for pq_file in sorted(month_dir.glob("*.parquet")):
                parquet_files.append(str(pq_file))

        if not parquet_files:
            self.logger.warning("Nenhum parquet encontrado em %s a partir de %04d-%02d", base_dir, start_year, start_month)
            return pd.DataFrame()

        dataset = ds.dataset(parquet_files, format="parquet")
        table = dataset.to_table(filter=ds.field("trade_flow") == flow)
        df = table.to_pandas()
        self.logger.info("Lidos %d arquivos parquet — flow=%s rows=%d", len(parquet_files), flow, len(df))
        return df

    def _collect_trade(self, flow: str) -> pd.DataFrame:
        """Locate parquet data, optionally scrape the next month, and return the trade DataFrame."""
        base_dir = self._scraper_parquet_dir()
        self.logger.info("Coletando %s da pasta %s", flow, base_dir)
        self._maybe_scrape_next_month(base_dir)
        return self._read_scraped_dataset(base_dir, flow)

    def collect_import_data(self) -> pd.DataFrame:
        """Return the import trade DataFrame."""
        return self._collect_trade("IMPORT")

    def collect_export_data(self) -> pd.DataFrame:
        """Return the export trade DataFrame."""
        return self._collect_trade("EXPORT")

    def normalize_columns(self, df: pd.DataFrame, contract: dict, import_export: int) -> pd.DataFrame:
        """Map raw scraper columns to pipeline-standard columns and drop rows with missing values."""
        if df is None or df.empty:
            return df

        work = df.copy()

        if "value_usd_million" in work.columns:
            work["valor_raw_usd"] = (
                pd.to_numeric(work["value_usd_million"], errors="coerce").astype("float64")
                * 1_000_000.0
            )

        if "year" in work.columns and "month" in work.columns:
            year_int = pd.to_numeric(work["year"], errors="coerce").astype("Int64")
            month_int = pd.to_numeric(work["month"], errors="coerce").astype("Int64")
            yyyymmdd = (year_int.astype("int64") * 10000 + month_int.astype("int64") * 100 + 1)
            work["Data"] = pd.to_datetime(yyyymmdd.astype(str), format="%Y%m%d", errors="coerce")

        if "hs_code" in work.columns:
            hs_numeric = pd.to_numeric(work["hs_code"], errors="coerce")
            work = work.loc[hs_numeric.notna()].copy()
            work["hs_code"] = hs_numeric.loc[work.index].astype("int64").astype(str)

        if "trade_flow" in work.columns:
            flow_map = {"IMPORT": 1, "EXPORT": 0}
            work["ImportExport"] = work["trade_flow"].astype(str).map(flow_map)

        work["frete"] = np.nan
        work["seguro"] = np.nan

        valor_col = next((c for c in ("valor_raw_usd", "valor") if c in work.columns), None)
        if valor_col:
            mask = pd.to_numeric(work[valor_col], errors="coerce").fillna(0) > 0
            dropped = int((~mask).sum())
            if dropped:
                self.logger.info("normalize_columns: %d linhas removidas por %s nulo/zero", dropped, valor_col)
            work = work.loc[mask].copy()

        peso_col = next((c for c in ("quantity", "peso") if c in work.columns), None)
        if peso_col:
            mask = pd.to_numeric(work[peso_col], errors="coerce").fillna(0) > 0
            dropped = int((~mask).sum())
            if dropped:
                self.logger.info("normalize_columns: %d linhas removidas por %s nulo/zero", dropped, peso_col)
            work = work.loc[mask].copy()

        return super().normalize_columns(work, contract, import_export)

    def load_ids_table(self) -> pd.DataFrame:
        """Load and filter IDS_comex.xlsx for India, returning NCM/ImportExport/IDIndicePrincipal columns."""
        # IND uses OM-era IDs (all < 382958) — skip the NM threshold applied by the base class.
        candidates = [
            Path("library/IDS_comex.xlsx"),
            _PROJECT_ROOT / "Comex-Derik" / "library" / "IDS_comex.xlsx",
            _PROJECT_ROOT / "library" / "IDS_comex.xlsx",
        ]
        ids_mapping = None
        for path in candidates:
            if path.exists():
                ids_mapping = pd.read_excel(path)
                self.logger.info("Tabela de IDs carregada de: %s", path.absolute())
                break
        if ids_mapping is None:
            raise FileNotFoundError("IDS_comex.xlsx não encontrado em nenhum caminho tentado")

        country_name = self._get_country_name()
        self.logger.info("Filtrando IDs para país: %s (sem filtro NM threshold)", country_name)
        ids_mapping = ids_mapping[ids_mapping["Pais_1"] == country_name].copy()
        if ids_mapping.empty:
            self.logger.warning("Nenhum ID encontrado para país '%s'", country_name)
            return pd.DataFrame(columns=["NCM", "ImportExport", "IDIndicePrincipal"])

        ids_mapping["NCM"] = pd.to_numeric(ids_mapping["NCM"], errors="coerce").astype("Int64")
        ids_mapping["ImportExport"] = pd.to_numeric(ids_mapping["ImportExport"], errors="coerce").astype("Int64")
        ids_mapping["IDIndicePrincipal"] = pd.to_numeric(ids_mapping["IDIndicePrincipal"], errors="coerce").astype("Int64")
        ids_mapping = ids_mapping.dropna(subset=["NCM", "ImportExport", "IDIndicePrincipal"])
        ids_mapping = ids_mapping[["NCM", "ImportExport", "IDIndicePrincipal"]].copy()
        ids_mapping.reset_index(drop=True, inplace=True)
        self.logger.info("IDs carregados: %d registros para %s)", len(ids_mapping), country_name)
        return ids_mapping

    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce India-specific column types and map ImportExport labels to numeric codes."""
        if df is None or df.empty:
            return df
        result = df.copy()
        if "ncm" in result.columns:
            ncm_numeric = pd.to_numeric(result["ncm"], errors="coerce")
            result = result.loc[ncm_numeric.notna()].copy()
            result["ncm"] = ncm_numeric.loc[result.index].astype("int64")
        if "pais_id" in result.columns:
            pid_numeric = pd.to_numeric(result["pais_id"], errors="coerce")
            result = result.loc[pid_numeric.notna()].copy()
            result["pais_id"] = pid_numeric.loc[result.index].astype("int64")
        if "ImportExport" in result.columns and result["ImportExport"].dtype == object:
            ie_map = {"Import": 1, "Export": 0}
            result["ImportExport"] = result["ImportExport"].map(ie_map)
        return result


def main(do_upload: bool = False, run_scraper: bool = False):
    """Run the India COMEX pipeline with a 5-month lookback from today."""
    today = datetime.now()
    start = (today.replace(day=1) - pd.DateOffset(months=5)).to_pydatetime()

    pipeline = COMEX_IND_NM_v2(start_date=start, developing=True, run_scraper=run_scraper)
    skip = [] if do_upload else ["upload"]
    pipeline.run(skip_phases=skip)
    return pipeline


if __name__ == "__main__":
    import sys as _sys
    do_upload = "--with-upload" in _sys.argv
    run_scraper = "--run-scraper" in _sys.argv
    p = main(do_upload=do_upload, run_scraper=run_scraper)
    n_silver = len(p.silver_df) if getattr(p, "silver_df", None) is not None else 0
    n_dropped = len(p.dropped_df) if getattr(p, "dropped_df", None) is not None else 0
    n_gold = len(p.gold_df) if getattr(p, "gold_df", None) is not None else 0
    print(
        f"\nPipeline IND v2 concluído (developing).\n"
        f"  silver:    {n_silver} linhas\n"
        f"  dropped:   {n_dropped} linhas\n"
        f"  gold v2:   {n_gold} linhas\n"
        f"  upload:    {'SIM' if do_upload else 'PULADO (use --with-upload para subir)'}"
    )
