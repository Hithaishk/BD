 import java.io.IOException;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.Mapper;
 import org.apache.hadoop.mapreduce.Reducer;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 public class StudentGrades{
 public static class GradeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 private Text studentName = new Text();
 private IntWritable score = new IntWritable();
 @Override
 protected void map(LongWritable key, Text value, Context context) throws 
IOException, InterruptedException{
 String line = value.toString();
 String[] fields = line.split(" ");
 studentName.set(fields[0]);
 score.set(Integer.parseInt(fields[1]));
 context.write(studentName, score);
 }
 }
 public static class GradeReducer extends Reducer<Text, IntWritable, Text, Text>{
 private Text grade = new Text();
 @Override
 protected void reduce(Text key, Iterable <IntWritable> values, Context context) 
throws IOException, InterruptedException{
 for(IntWritable value: values){
 int score = value.get();
 if(score >= 90){
 grade.set("A");
 } else if( score >= 80){
 grade.set("B");
 } else if( score >= 70){
 grade.set("C");
 } else if(score >= 60){
 grade.set("D");
 } else{
 grade.set("F");
 }
 context.write(key, grade);
 }
 }
 }
public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "Student Grades");
 job.setJarByClass(StudentGrades.class);
 job.setMapperClass(GradeMapper.class);
 job.setReducerClass(GradeReducer.class);
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(IntWritable.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
 }
