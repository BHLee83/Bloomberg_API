@echo off

set today=%date:~0,4%%date:~5,2%%date:~8,2%

cd /d D:\blpdaemon

echo.
echo ### Start bat... >> .\log\bloomberg-ivol-%today%.log

C:\Users\USER\Anaconda3\python.exe insert_rt_ticker_from_blp.py >> .\log\bloomberg-ivol-%today%.log

echo Return "%errorlevel%" >> .\log\bloomberg-ivol-%today%.log

echo ### End bat... >> .\log\bloomberg-ivol-%today%.log
echo.

exit /b %errorlevel%
