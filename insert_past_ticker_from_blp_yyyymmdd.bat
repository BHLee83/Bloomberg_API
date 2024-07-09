@echo off

set today=%date:~0,4%%date:~5,2%%date:~8,2%

cd /d D:\blpdaemon

echo.
echo ### Start bat... >> .\log\bloomberg-ivol-%today%.log

C:\Users\USER\Anaconda3\python.exe insert_past_ticker_from_blp_yyyymmdd.py >> .\log\bloomberg-past-%today%.log

echo Return "%errorlevel%" >> .\log\bloomberg-ivol-%today%.log

echo ### End bat... >> .\log\bloomberg-ivol-%today%.log
echo.

exit /b %errorlevel%
