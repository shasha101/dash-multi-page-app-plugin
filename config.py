from datetime import datetime, tzinfo, timezone, timedelta, date
import pytz


''' DATETIME NOW '''
localTimezone = pytz.timezone('Asia/Singapore')
datetimeNow = datetime.now(localTimezone)

smartgym_start = datetime(2020, 6, 1, 0, 0, 0, tzinfo=timezone.utc)