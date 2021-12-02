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

public class Student {
    
    public static class testMap_institute extends Mapper<LongWritable,Text, Text, IntWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
            String line = values.toString();
            String[] insti = line.split(" ");
            //String insti = line.substring(0,3);
            String par = context.getConfiguration().get("INSTITUTE");
            if(insti[1].equals(par))
            {
            context.write(values,new IntWritable(1));
            }
    }}
  
    public static class testMap_program extends Mapper<LongWritable,Text, Text, IntWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
            String line = values.toString();
            String[] program = line.split(" ");
            String par = context.getConfiguration().get("PROGRAM");
            if(program[2].equals(par)){
            context.write(new Text(program[2]),new IntWritable(1));
            }
    }}
   
    public static class testMap_gender extends Mapper<LongWritable,Text, Text, IntWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
            String line = values.toString();
            String[] gender = line.split(" ");
            String par = context.getConfiguration().get("GENDER");
            if(gender[3].equals(par)){
            context.write(new Text(gender[3]),new IntWritable(1));
            }
    }}
    public static class testMap_instigender extends Mapper<LongWritable,Text, Text, IntWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
            String line = values.toString();
            String[] insti = line.split(" ");
            String par = context.getConfiguration().get("INSTITUTE");
            if(insti[1].equals(par)){
            context.write(new Text(insti[3]),new IntWritable(1));
            }
    }}
    public static class testReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        	int totalCount = 0;
			for(IntWritable x: values) {
				totalCount += x.get();
			}
			context.write(key, new IntWritable(totalCount));
        }
        
    }
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        conf.set("INSTITUTE", args[5]);
        Job job = Job.getInstance(conf,"student_institute");
        job.setJarByClass(studentDetails2.class);
        job.setMapperClass(testMap_institute.class);
        job.setReducerClass(testReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
        Configuration conf1 = new Configuration();
        conf1.set("PROGRAM", args[5]);
        Job job1 = Job.getInstance(conf1,"student_program");
        job1.setJarByClass(studentDetails2.class);
        job1.setMapperClass(testMap_program.class);
        job1.setReducerClass(testReduce.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        job1.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        conf2.set("GENDER", args[5]);
        Job job2 = Job.getInstance(conf2,"student_gender");
        job2.setJarByClass(studentDetails2.class);
        job2.setMapperClass(testMap_gender.class);
        job2.setReducerClass(testReduce.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job2,new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        
        job2.waitForCompletion(true);
        Configuration conf3 = new Configuration();
        conf3.set("INSTITUTE", args[5]);
        Job job3 = Job.getInstance(conf3,"student_instigender");
        job3.setJarByClass(studentDetails2.class);
        job3.setMapperClass(testMap_instigender.class);
        job3.setReducerClass(testReduce.class);
        
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job3,new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        
        job3.waitForCompletion(true);
        System.exit(job3.waitForCompletion(true)?0:1);
        
    }
}