# Design Decisions - Argentina Chain Tracker

## Source: Bull Market Brokers Competitive Analysis (2026-02-12)

### ADOPTED FROM BROKER CONVENTIONS
1. **Tabular numerals (monospace) for all financial data**  
   JetBrains Mono already in design system. Decimal alignment mandatory.

2. **Argentine number formatting**  
   Dot for thousands separator, comma for decimals: $1.280,00  
   Percentages: 17,4% (comma decimal)  
   Confirm with operator (Lau) - this is the local standard.

3. **Variation indicators are expected**  
   Every price/value that changes daily MUST show: direction (arrow) + magnitude.  
   Users trained by brokers to expect this. Missing it feels broken.

4. **Source attribution on every data point**  
   Brokers show "BYMA" or "MAE" as implicit source. We show it explicitly
   with credibility tier. Same principle, higher transparency.

### INTENTIONALLY DIFFERENT FROM BROKER CONVENTIONS
1. **Warm palette instead of red/green**  
   Brokers use #27ae60 (green) and #e74c3c (red). We use Sage (#8B9E82)
   and Soft Terracotta (#C17F59). Deliberate signal: "this is not a
   trading terminal." First-time broker users will need ~30 seconds to
   recalibrate. Acceptable tradeoff for daily comfort.

2. **Card-based overview instead of table-based density**  
   Brokers show 20+ tickers in table format. We show 5-6 macro groups
   in cards. Different information architecture: macro intelligence
   vs. individual asset monitoring.  
   EXCEPTION: if we add a Markets detail page with individual
   stocks/bonds, switch to table-based density for that view.

3. **Causal chain as primary navigation metaphor**  
   Brokers navigate by asset class (Acciones, Bonos, CEDEARs).  
   We navigate by causal layer (Global, Transmission, Monetary,
   Markets, Regulatory). Our organizing principle is causation,
   not instrument type.

4. **No order book / no trading functionality**  
   We never show bid/ask. We never enable order placement.  
   We are read-only intelligence. This removes 80% of broker
   UI complexity and lets us use that space for context and analysis.

### FUTURE CONSIDERATIONS (NOT YET IMPLEMENTED)
1. **Temporal relevance for licitaciones**  
   On LECAP/LECER auction days, surface auction data prominently
   in Overview. BMB does this with a dedicated section. We should
   do it with a temporary card that appears only on auction days.

2. **Dual settlement pricing (CI vs 24hs)**  
   If we ever display individual bond prices, show both settlement
   terms. Argentine users expect this. Not relevant for Phase 1 macro view.

3. **CEDEAR premium calculation**  
   CEDEAR price = f(underlying USD price, CCL rate, conversion ratio).  
   BMB shows ratio. PPI and Rava show implied CCL.  
   If we add CEDEAR tracking, show the decomposition:
   underlying price + implied CCL + ratio = CEDEAR price.  
   This teaches the user HOW the price is constructed.

4. **PPI-style bond analysis**  
   PPI is the gold standard for fixed income. If we add bond analysis,
   study PPI's calculators and debt map approach specifically.  
   Source: https://www.portfoliopersonal.com/

5. **Table mode for dense data views**  
   Card layout for macro Overview. Table layout for any future
   individual-asset views. Both patterns available, right context.

### COMPETITIVE POSITIONING SUMMARY
We are not competing with Bull Market, Cocos, PPI, or any broker.  
We are the LAYER ABOVE them - the intelligence system that helps
users understand WHY the prices they see on their broker are moving.

Our user opens Bull Market to trade.  
Our user opens Chain Tracker to UNDERSTAND.

Both tools, one workflow. Complementary, not competitive.
