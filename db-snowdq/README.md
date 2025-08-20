# 📘 Snowflake Data Quality & Documentation Platform

A comprehensive Streamlit application for enhancing data governance in Snowflake through AI-powered documentation, automated data quality monitoring, and contact management. Optimized for **Streamlit in Snowflake (SiS)** with full local development support.


[![Watch the video](https://img.youtube.com/vi/bF6FAMeGEZc/maxresdefault.jpg width="50%")](https://youtu.be/bF6FAMeGEZc)



## 🌟 Key Highlights

- **🤖 AI-Powered Documentation** - Generate intelligent descriptions using Snowflake Cortex LLMs
- **🔍 Data Quality Monitoring** - Comprehensive DMF setup and monitoring dashboard
- **👥 Contact Management** - Assign data stewards, support contacts, and access approvers
- **📊 Executive Dashboard** - Real-time KPIs and governance metrics
- **🏗️ SiS Optimized** - Native compatibility with Streamlit in Snowflake environments
- **📈 Historical Tracking** - Complete audit trail of all changes and improvements

## 🚀 Features

### 📝 AI-Powered Data Documentation
- **Multi-Model Support**: Choose from `claude-4-sonnet`, `mistral-large2`, `llama3-70b`, `snowflake-arctic`, and `snowflake-llama-3.1-405b`
- **Smart Context Analysis**: Analyzes table structure, column types, and sample data for accurate descriptions
- **Bulk Operations**: Generate descriptions for multiple tables and columns simultaneously
- **View Support**: Advanced DDL parsing and recreation for view column descriptions
- **Character Limits**: Optimized descriptions (150 chars for tables, 100 chars for columns)
- **Real-time Preview**: View generated descriptions before applying

### 🔍 Advanced Data Quality Monitoring
- **Comprehensive DMF Support**: 
  - Table-level: `ROW_COUNT`, `FRESHNESS`
  - Column-level: `NULL_COUNT`, `NULL_PERCENT`, `DUPLICATE_COUNT`, `UNIQUE_COUNT`, `ACCEPTED_VALUES`, `AVG`, `MAX`, `MIN`, `STDDEV`
- **Flexible Scheduling**: Periodic (minutes/hours), daily, or trigger-on-changes
- **Bulk Configuration**: Apply metrics to all columns or configure individually
- **SQL Generation**: Download ready-to-execute SQL scripts
- **Results Dashboard**: Comprehensive monitoring with filters and KPIs
- **Historical Analysis**: Track quality trends over time

### 👥 Data Governance & Contacts
- **Contact Types**: Data Steward, Technical Support, Access Approver
- **Current Assignments**: View existing contacts for any table
- **Pre-populated Forms**: Automatically populate dropdowns with existing assignments
- **SQL Generation**: Generate `ALTER TABLE SET CONTACT` statements
- **Integration**: Seamless integration with Snowflake's contact system

### 📊 Executive Dashboard & KPIs
- **Real-time Metrics**: 
  - Total Databases, Schemas, Tables & Views
  - Documentation coverage percentage
  - Active data quality monitors
  - Contact assignments
- **Visual KPI Cards**: Modern, gradient-styled metric displays
- **Manual Refresh**: Force refresh of all KPIs from Snowflake
- **Trend Analysis**: Monitor improvements over time

### 🏗️ Streamlit in Snowflake (SiS) Compatibility
- **INFORMATION_SCHEMA Queries**: Primary approach for better permission compatibility
- **Fallback Mechanisms**: Automatic fallback to SHOW commands when needed
- **Owner's Rights Model**: Handles SiS permission limitations gracefully
- **Consistent Results**: Reliable behavior across SiS and standalone environments
- **Debug Information**: Helpful error messages and troubleshooting guidance

### 📈 Comprehensive History & Audit
- **Description History**: Track all table, view, and column description changes
- **DMF Configuration History**: Monitor data quality setup changes
- **Contact Assignment History**: Audit trail for governance assignments
- **Export Capabilities**: Download history as CSV for compliance reporting
- **User Attribution**: Track who made what changes when

## 📁 Project Structure

```
db-snowdq/
├── streamlit_app.py              # Main single-page application
├── streamlit_app_backup.py       # Clean backup version
├── environment.yml               # SiS-compatible dependencies
├── requirements.md               # Detailed requirements specification
├── README.md                     # This documentation
├── pages/                        # Legacy multi-page components (reference)
│   ├── data_contacts.py
│   ├── data_descriptions.py
│   ├── data_quality.py
│   ├── documentation.py
│   └── history.py
├── utils/                        # Utility modules (reference)
│   ├── cortex_utils.py          # LLM integration
│   ├── db_setup.py              # Database setup
│   └── snowflake_utils.py       # Metadata operations
└── sql/
    ├── setup_db_snowtools.sql      # Database setup script
    └── setup_app_permissions.sql   # Permissions setup script
```

## 🔧 Quick Start

### Option 1: Streamlit in Snowflake (Recommended)
1. **Upload** `streamlit_app.py` and `environment.yml` to your Snowflake stage
2. **Create** the Streamlit app in Snowflake
3. **Run** the app - it will auto-setup required database objects
4. **Grant permissions** as needed (see permissions section)

### Option 2: Local Development
1. **Clone** this repository
2. **Install** dependencies: `pip install -r requirements.txt`
3. **Configure** Snowflake connection in `~/.snowflake/connections.toml`
4. **Run**: `streamlit run streamlit_app.py`

### Automatic Setup
The app automatically creates required database objects on first run:
- ✅ `DB_SNOWTOOLS` database
- ✅ `DATA_DESCRIPTION_HISTORY` table
- ✅ `DATA_QUALITY_RESULTS` table

## 🔐 Required Permissions

### Core System Access
```sql
-- Metadata and system access
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE your_role;
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE your_role;

-- Cortex LLM access for AI descriptions
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE your_role;

-- Data quality monitoring
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE your_role;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE your_role;
```

### Data Access (Customize for Your Databases)
```sql
-- Grant access to your data databases
GRANT USAGE ON DATABASE your_database TO ROLE your_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE your_database TO ROLE your_role;
GRANT SELECT ON ALL TABLES IN DATABASE your_database TO ROLE your_role;
GRANT SELECT ON ALL VIEWS IN DATABASE your_database TO ROLE your_role;

-- For description updates
GRANT MODIFY ON ALL TABLES IN DATABASE your_database TO ROLE your_role;
GRANT MODIFY ON ALL VIEWS IN DATABASE your_database TO ROLE your_role;

-- For contact management
GRANT REFERENCES ON ALL TABLES IN DATABASE your_database TO ROLE your_role;
```

### DMF Setup (For Data Quality)
```sql
-- For setting up Data Metric Functions
GRANT OWNERSHIP ON TABLE your_database.your_schema.your_table TO ROLE your_role;
-- OR
GRANT ALL PRIVILEGES ON TABLE your_database.your_schema.your_table TO ROLE your_role;
```

## 📖 User Guide

### 🏠 Home Dashboard
- **KPI Overview**: View real-time governance metrics
- **Quick Actions**: Navigate directly to key features
- **System Information**: Connection details and platform overview
- **Setup Status**: Verify database objects are configured

### 📝 Data Descriptions
1. **Select Database/Schema**: Choose your target objects
2. **Filter Objects**: Show only undocumented items
3. **Choose LLM Model**: Select from available Cortex models
4. **Select Objects**: Use checkboxes to choose tables/views
5. **Generate Descriptions**: Choose table, column, or both
6. **Review Results**: View generated descriptions in collapsible sections
7. **Refresh Data**: Use the refresh button to see applied changes

### 🔍 Data Quality
1. **Select Target Table**: Choose database, schema, and table
2. **Set Schedule**: Configure monitoring frequency
3. **Choose DMFs**: Select table-level and column-level metrics
4. **Generate SQL**: Download or apply DMF configuration
5. **Monitor Results**: View quality check results and trends

### 👥 Data Contacts
1. **Select Table**: Choose your target table
2. **View Current Contacts**: See existing assignments
3. **Update Assignments**: Set steward, support, and approver contacts
4. **Apply Changes**: Execute generated SQL or download for later

### 📈 History
- **Description History**: Track all documentation changes
- **Quality History**: Comprehensive DMF monitoring dashboard
- **Export Options**: Download history data for reporting

## 🔍 Technical Architecture

### SiS Compatibility Features
- **Primary Queries**: Uses `INFORMATION_SCHEMA` views for consistent results
- **Fallback System**: Automatic fallback to `SHOW` commands when needed
- **Permission Handling**: Graceful degradation with helpful error messages
- **Debug Mode**: Detailed logging for troubleshooting permission issues

### Performance Optimizations
- **Intelligent Caching**: `@st.cache_data` with TTL for optimal performance
- **Batch Operations**: Efficient bulk processing for large datasets
- **Minimal Warehouse Usage**: Optimized for small warehouse compatibility
- **Memory Management**: Designed for SiS 32MB data transfer limits

### Security & Compliance
- **Role-Based Access Control**: Follows Snowflake RBAC best practices
- **Complete Audit Trail**: All changes tracked with user attribution
- **Data Privacy**: No external data transfers (runs entirely in Snowflake)
- **Permission Isolation**: Clear separation between app and user data access

## 🚨 Troubleshooting

### Common SiS Issues

**Tables/Columns Not Displaying**
- ✅ **Fixed**: Now uses `INFORMATION_SCHEMA` queries for better compatibility
- Check app owner has proper database access permissions
- Verify `INFORMATION_SCHEMA` access is available

**Permission Errors**
- Ensure app owner role has required system privileges
- Grant `USAGE` on target databases and schemas
- Verify Cortex and DMF roles are properly assigned

**DMF Setup Failures**
- DMFs require table ownership or full privileges
- Run generated SQL with appropriate role
- Check that schedules are properly configured

### Performance Issues
- Use smaller warehouse for better cost efficiency
- Enable caching by avoiding frequent page refreshes
- Filter to specific databases/schemas for large environments

### Model Availability
- Check available models: `SELECT * FROM SNOWFLAKE.CORTEX.COMPLETE_AVAILABLE_MODELS()`
- Verify account region supports selected models
- Try different models if one is unavailable

## 🔄 Recent Updates

### v2.0 - SiS Compatibility & Enhanced Features
- ✅ **SiS Optimization**: Full compatibility with Streamlit in Snowflake
- ✅ **INFORMATION_SCHEMA**: Primary queries for better permission handling
- ✅ **Enhanced UI**: Modern KPI dashboard and improved navigation
- ✅ **Contact Management**: Complete data governance contact system
- ✅ **Advanced DMF**: Comprehensive data quality monitoring dashboard
- ✅ **History Tracking**: Complete audit trail for all operations
- ✅ **Error Handling**: Graceful degradation with helpful messages

### v1.0 - Core Features
- AI-powered description generation
- Basic DMF setup
- Multi-page navigation
- Local development support

## 🤝 Contributing

This application follows Snowflake best practices and is designed for easy extension:

- **Add LLM Models**: Update `AVAILABLE_MODELS` list and test compatibility
- **Extend DMF Support**: Add new metric types in the DMF configuration section
- **New Features**: Follow the established pattern of primary/fallback queries
- **UI Improvements**: Maintain the modern, gradient-styled design system

## 📊 Best Practices

### For Data Stewards
1. Start with high-value, frequently-used tables
2. Use consistent description styles across your organization
3. Review and refine AI-generated descriptions
4. Set up data quality monitoring on critical tables
5. Assign contacts for clear ownership

### For Administrators
1. Grant minimal required permissions initially
2. Monitor Cortex usage for cost management
3. Set up appropriate DMF schedules (avoid over-monitoring)
4. Use the history tracking for compliance reporting
5. Regular backup of `DB_SNOWTOOLS` database

### For Developers
1. Test in both SiS and local environments
2. Use the debug information for troubleshooting
3. Follow the established error handling patterns
4. Maintain backward compatibility when extending features

## 📞 Support & Resources

- **In-App Documentation**: Check the Documentation tab within the app
- **Setup Scripts**: Review `sql/setup_*.sql` files for manual setup
- **Troubleshooting**: Use the debug information provided by the app
- **Permissions**: Verify against the detailed permissions section above

---

**🎯 Built for Modern Data Teams** - Transform your Snowflake environment into a well-documented, high-quality data platform with AI-powered automation and comprehensive governance tools!

**🔺 Snowflake Native** - Runs entirely within your Snowflake environment with no external dependencies or data transfers.
