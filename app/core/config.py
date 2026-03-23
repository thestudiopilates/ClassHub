from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Momence Ops API"
    api_prefix: str = "/v1"
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/momence_ops"
    momence_base_url: str = "https://api.momence.com"
    momence_client_id: str = ""
    momence_client_secret: str = ""
    momence_redirect_uri: str = "http://127.0.0.1:8000/v1/auth/momence/callback"
    momence_oauth_scopes: str = ""
    momence_token_store_path: str = ".momence_tokens.json"
    momence_username: str = ""
    momence_password: str = ""
    default_timezone: str = "America/New_York"
    momence_host_id: int = 29863
    momence_browser_profile_dir: str = ""
    momence_birthdays_report_url: str = ""
    momence_customer_list_report_url: str = ""
    momence_customer_attendance_report_url: str = ""
    momence_session_bookings_report_url: str = ""
    momence_customer_field_values_report_url: str = ""
    momence_allow_broad_context_sync: bool = False
    momence_max_context_refresh_batch: int = 25
    momence_session_bookings_csv_path: str = ""
    momence_allow_browser_booking_report_sync: bool = False
    momence_upcoming_booking_days: int = 7
    momence_history_booking_days: int = 60
    momence_history_booking_chunk_days: int = 1
    momence_enable_check_in_write: bool = False
    ops_roster_history_batch_size: int = 15
    ops_roster_history_pause_seconds: float = 0.3
    ops_auto_warm_enabled: bool = False
    ops_auto_warm_max_batches: int = 4
    ops_auto_warm_day_offset: int = 0


settings = Settings()
