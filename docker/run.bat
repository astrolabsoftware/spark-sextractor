
set MOUNT=-v d:\workspace:/work
rem set MOUNT=-v d:\workspace\ji2018\IVY:/work/IVY

call docker\image.bat

docker run --rm -it %MOUNT% -v /tmp/.X11-unix:/tmp/.X11-unix %IMAGE% bash
