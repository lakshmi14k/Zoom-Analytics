**End-to-End B2B SaaS Analytics (Zoom)**

**Problem Statement** : Enterprise Zoom customers underutilize advanced features (Webinars, Large Meetings, Breakout Rooms) despite paying for them, 
leading to churn risk and missed expansion opportunities.

**Proposed Solution** : An analytics pipeline that cleans messy data, calculates utilization metrics, and provides actionable recommendations for Sales/Customer Success teams.

**Key Findings**

- $2.45M annual revenue at risk** across 311 accounts (88% of portfolio)
- 290 accounts need seat consolidation** discussions
- Average seat utilization: 11.47%** - massive optimization opportunity
- Clear action plan: Consolidate Seats (290), Feature Training (4), Re-engage (6)

**Dataset Overview** : Synthetic dataset simulating realistic B2B SaaS operational data with intentional data quality issues.
- **Total Records:** ~14,000 rows  
- **Time Period:** 3 months of event data (July-October 2024)  
- **Accounts:** 400 raw → 353 after deduplication

**Tech Stack**
- **Database** : PostgreSQL (via Docker)
- **Transformation** : dbt Core
- **Visualization** : Tableau
- **Data** : 14K+ rows across 4 tables

**Project Structure**

├── data/              # Raw CSV files

├── zoom_analytics/    # dbt project (staging → intermediate → marts)

├── dashboard/         # Tableau workbook

└── docs/              # Detailed documentation

**Data Lineage**
<img width="2416" height="770" alt="image" src="https://github.com/user-attachments/assets/5c14ffb8-d460-41f1-8d88-89ddca6c5a30" />

**Installation Steps:**

 - Start PostgreSQL : docker run --name zoom-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:14
 - Install dbt : pip install dbt-postgres
 - Run pipeline : cd zoom_analytics dbt run



**Results**
- 353 accounts analyzed
- 4 staging models (data cleaning)
- 3 intermediate models (business logic)
- 1 mart model (39 metrics)
- Actionable insights for 311 at-risk accounts

*Built with Docker, PostgreSQL, dbt, and Tableau*

See [detailed documentation] for complete setup instructions.
