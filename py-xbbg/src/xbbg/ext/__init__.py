"""Extension functions for xbbg.

This package re-exports the public extension helpers lazily so importing
one pure-Python symbol does not eagerly pull in every native-backed module.
"""

from __future__ import annotations

import importlib

__all__ = [
    # Historical extensions (sync)
    "dividend",
    "earnings",
    "turnover",
    "etf_holdings",
    # Historical extensions (async)
    "adividend",
    "aearnings",
    "aturnover",
    "aetf_holdings",
    # Futures extensions (sync)
    "fut_ticker",
    "active_futures",
    "cdx_ticker",
    "active_cdx",
    # Futures extensions (async)
    "afut_ticker",
    "aactive_futures",
    "acdx_ticker",
    "aactive_cdx",
    # Currency extensions (sync)
    "convert_ccy",
    # Currency extensions (async)
    "aconvert_ccy",
    # Fixed income extensions (sync)
    "yas",
    "YieldType",
    "preferreds",
    "corporate_bonds",
    "bqr",
    # Fixed income extensions (async)
    "ayas",
    "apreferreds",
    "acorporate_bonds",
    "abqr",
    # Bond analytics (sync)
    "bond_info",
    "bond_risk",
    "bond_spreads",
    "bond_cashflows",
    "bond_key_rates",
    "bond_curve",
    # Bond analytics (async)
    "abond_info",
    "abond_risk",
    "abond_spreads",
    "abond_cashflows",
    "abond_key_rates",
    "abond_curve",
    # Options analytics enums
    "PutCall",
    "ChainPeriodicity",
    "StrikeRef",
    "ExerciseType",
    "ExpiryMatch",
    # Options analytics (sync)
    "option_info",
    "option_greeks",
    "option_pricing",
    "option_chain",
    "option_chain_bql",
    "option_screen",
    # Options analytics (async)
    "aoption_info",
    "aoption_greeks",
    "aoption_pricing",
    "aoption_chain",
    "aoption_chain_bql",
    "aoption_screen",
    # CDX analytics (sync)
    "cdx_info",
    "cdx_defaults",
    "cdx_pricing",
    "cdx_risk",
    "cdx_basis",
    "cdx_default_prob",
    "cdx_cashflows",
    "cdx_curve",
    # CDX analytics (async)
    "acdx_info",
    "acdx_defaults",
    "acdx_pricing",
    "acdx_risk",
    "acdx_basis",
    "acdx_default_prob",
    "acdx_cashflows",
    "acdx_curve",
]

_ATTR_TO_MODULE = {
    # bonds
    "abond_cashflows": "bonds",
    "abond_curve": "bonds",
    "abond_info": "bonds",
    "abond_key_rates": "bonds",
    "abond_risk": "bonds",
    "abond_spreads": "bonds",
    "bond_cashflows": "bonds",
    "bond_curve": "bonds",
    "bond_info": "bonds",
    "bond_key_rates": "bonds",
    "bond_risk": "bonds",
    "bond_spreads": "bonds",
    # cdx
    "acdx_basis": "cdx",
    "acdx_cashflows": "cdx",
    "acdx_curve": "cdx",
    "acdx_default_prob": "cdx",
    "acdx_defaults": "cdx",
    "acdx_info": "cdx",
    "acdx_pricing": "cdx",
    "acdx_risk": "cdx",
    "cdx_basis": "cdx",
    "cdx_cashflows": "cdx",
    "cdx_curve": "cdx",
    "cdx_default_prob": "cdx",
    "cdx_defaults": "cdx",
    "cdx_info": "cdx",
    "cdx_pricing": "cdx",
    "cdx_risk": "cdx",
    # currency
    "aconvert_ccy": "currency",
    "convert_ccy": "currency",
    # fixed income
    "YieldType": "fixed_income",
    "abqr": "fixed_income",
    "acorporate_bonds": "fixed_income",
    "apreferreds": "fixed_income",
    "ayas": "fixed_income",
    "bqr": "fixed_income",
    "corporate_bonds": "fixed_income",
    "preferreds": "fixed_income",
    "yas": "fixed_income",
    # futures
    "aactive_cdx": "futures",
    "aactive_futures": "futures",
    "acdx_ticker": "futures",
    "active_cdx": "futures",
    "active_futures": "futures",
    "afut_ticker": "futures",
    "cdx_ticker": "futures",
    "fut_ticker": "futures",
    # historical
    "adividend": "historical",
    "aearnings": "historical",
    "aetf_holdings": "historical",
    "aturnover": "historical",
    "dividend": "historical",
    "earnings": "historical",
    "etf_holdings": "historical",
    "turnover": "historical",
    # options
    "ChainPeriodicity": "options",
    "ExerciseType": "options",
    "ExpiryMatch": "options",
    "PutCall": "options",
    "StrikeRef": "options",
    "aoption_chain": "options",
    "aoption_chain_bql": "options",
    "aoption_greeks": "options",
    "aoption_info": "options",
    "aoption_pricing": "options",
    "aoption_screen": "options",
    "option_chain": "options",
    "option_chain_bql": "options",
    "option_greeks": "options",
    "option_info": "options",
    "option_pricing": "options",
    "option_screen": "options",
}


def __getattr__(name: str):
    """Resolve extension exports lazily to avoid import-time native coupling."""
    module_name = _ATTR_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f"xbbg.ext.{module_name}")
    return getattr(module, name)


def __dir__() -> list[str]:
    """Expose public exports for tab completion."""
    return list(__all__)
