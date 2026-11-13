# Basic analytics

User scope: implement basic descriptive analysis now, without AI. Keep existing Streamlit and warehouse architecture.

Build a read-only basic analytics page over one scoped warehouse snapshot: current catalog counts, collected/valid review counts and weighted average, 1–5 star distribution, brand/category comparisons, sample-qualified product rankings, calendar review trends and observed daily-average price changes. Export computed tables as safe UTF-8 CSV. Reuse existing trend calculations. No external model, training, forecasting or new ingestion.

Choose an integrated page with a pure pandas report service. Extending scattered charts alone would not provide consistent filters; replacing the frontend/API is outside this step. User authorized implementation directly; routine UI choices are implementer decisions.

Filter one source group and selected brands/categories consistently across inputs. Join reviews by product ID and source group; reject ambiguous catalog identities. Missing currency cannot enter monetary comparisons; known currencies remain separate. Optional inclusive UTC date bounds filter review dates and price observation dates, while catalog values describe the current catalog, not historical stock. Missing dates remain visible as exclusions. A single price date never produces a percentage change. Compare first/last observed daily averages, without interpolating missing days or interpreting review volume as sales.

Use the existing dashboard styling/navigation. Add meaningful independent arithmetic tests and Streamlit interaction tests, then local regression and read-only real warehouse smoke. Preserve baseline Grafana/user files and persistent service data.
