package perf.spark.elastic.perf.spark.elastic;

public class TestFunctionsMain {

	public static void main(String[] args) {
		System.out.println(" This is the start of test function main");

		String logContents = "PermissionSpec{permType='DATA_._MODEL , SYS, , , ID, , , , , , , Publishing, event, to, topic-com,";

		String cleanedContents = CommonUtil.cleanUnusedChars(logContents);

		System.out.println("This is the cleaned contents " + cleanedContents);

		// "Start, request, , , sf, goals, , , , , , , oldState, =, objectives, ,
		// newState, =, objectives, , , , , , , Overwriting, target, target,
		// population?, false, , , , , , , sqlb=SQLWithBindVars, sql=, SELECT, USERS, ,
		// , SYS, , , ID, , , , , , , Publishing, event, to, topic-com,
		String textStr = "Start, request, , , sf, goals, , , , , , , oldState, =, objectives, , newState, =, objectives, , , , , , , Overwriting, target, target, population?, false, , , , , , ,";
		textStr = textStr.replaceAll("\\,", " ");
		
		System.out.println("after replace " + textStr);
	}

}
