Project version: 2021-04-28.

Example usage using the provided test files:

  cd <project-root>
  mvn clean package

Running jar require two properties:

-Dinput="{file path with extension in psv format, i.e. 'files/input.psv' }"
-Doutput="{only file name without extension, i.e. 'output' }"

additionally you can specify spark cluster address with -DsparkAddress, if you won't program will use "local[*]"

java -jar -Dinput="" -Doutput="" target/SessionsJob-1.0.jar 'src/test/resources/input-statements.psv' 'target/actual-sessions.psv'

!When running on Windows with local(default) Spark cluster there is need for more setup:
https://phoenixnap.com/kb/install-spark-on-windows-10 - 6th step and 9. point from 7th step are crucial

I'm aware that writing result to file should be improved because ``` repartition(1) ``` is not efficient.
Of course for final version there should be more specific unit tests.
