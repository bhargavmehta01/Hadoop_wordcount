package hadp;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class reducefn extends Reducer<Text,IntWritable,Text,Text> {

	Map<String, Map<String, Integer>> outrmp = new HashMap<String, Map<String,Integer>>();
	Map<String, String> statesbucket = new HashMap<String, String>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws 
		IOException, InterruptedException 
	{		
		int sum = 0;
		for (IntWritable val : values) 
		{
			sum += val.get();
		}	
		statesbucket.put(key.toString(), sum+"");
		
	}


	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		for(Entry<String, String> entry1 : statesbucket.entrySet())
		{
			String keytem[] = entry1.getKey().toString().split(":");
			
			String state = keytem[0];
			String wrd = keytem[1];
			int count = Integer.parseInt(entry1.getValue());
			
			if(!outrmp.containsKey(state)){
				Map<String, Integer> inrmp = new HashMap<String, Integer>();
				inrmp.put(wrd, count);
				outrmp.put(state, inrmp);
			}
			else{
				Map<String, Integer> temp = null;
				temp = outrmp.get(state);
				if(!temp.containsKey(wrd))
				{
					temp.put(wrd, count);
				}
				outrmp.put(state, temp);
			}
		}
		
		Set sts = outrmp.entrySet();
		Iterator it = sts.iterator();
		
		while(it.hasNext()){
			Map<String, Integer> temp1 = null;
			Map.Entry me = (Map.Entry)it.next();
			
			temp1 = (Map<String, Integer>) me.getValue();
			
			List<Entry<String, Integer>> templist = new LinkedList<Entry<String, Integer>>(temp1.entrySet());
			
			Collections.sort(templist, new Comparator<Entry<String, Integer>>() {
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			
			Map<String, Integer> sortedmp = new LinkedHashMap<String, Integer>();
			for(Entry<String, Integer>entry:templist)
			{
				sortedmp.put(entry.getKey(), entry.getValue());
			}
			outrmp.put((String)me.getKey(),sortedmp);
			}
		//System.out.println("ANSWER B - i) : \n");
		context.write(new Text("ANSWER B - i) : \n"), new Text(""));
		//System.out.println("Number of appearances of each word in each file are as shown below : ");
		context.write(new Text("Number of appearances of each word in each file are as shown below : "), new Text(""));
		for( String key : outrmp.keySet()){
			//System.out.println(key + " : " + outrmp.get(key));
			context.write(new Text(key), new Text(" : "+outrmp.get(key)));
		}
		
		int agr = 0;
		int edu = 0;
		int pol = 0;
		int spr = 0;
	
		sts = outrmp.entrySet();
		it = sts.iterator();
		while(it.hasNext())
		{
			Map<String, Integer> temp1 = null;
			Map.Entry entry1 = (Map.Entry)it.next();
			
			temp1= (HashMap<String, Integer>) entry1.getValue();
			Map.Entry entry2 = temp1.entrySet().iterator().next();
			
			if(entry2.getKey().equals("agriculture"))
			{
				agr+=1;
			}
			else if (entry2.getKey().equals("education")) {
				edu+=1;			
			}
			else if (entry2.getKey().equals("politics")) {
				pol+=1;
			}
			else if(entry2.getKey().equals("sports")){
				spr+=1;
			}
		}
		context.write(new Text("\nThe number of states for which 'sports' is dominant than others are : "), new Text(spr+""));
		context.write(new Text("The number of states for which 'education' is dominant than others are : "), new Text(edu+""));
		context.write(new Text("The number of states for which 'education' is dominant than others are : "), new Text(agr+""));
		context.write(new Text("The number of states for which 'politics' is dominant than others are : "), new Text(pol+""));
//		System.out.println("The number of states for which 'education' is dominant than others are : "+edu);
//		System.out.println("The number of states for which 'agriculture' is dominant than others are : "+agr);
	//	System.out.println("The number of states for which 'politics' is dominant than others are : "+pol);
		
		
		// For Answer B - ii)
		Map <String, String> aggrgt = new HashMap<String, String>();
		sts = outrmp.entrySet();		
		Iterator itd2 = sts.iterator();
		while(itd2.hasNext())
		{
			Map.Entry entr1 = (Map.Entry)itd2.next();
			Map<String, Integer> temp2 = null;
			
			temp2=(HashMap<String, Integer>) entr1.getValue();
			String alph = "";
			
			Set sts1 = temp2.entrySet();
			Iterator itd3 = sts1.iterator();
			while(itd3.hasNext())
			{
				Map.Entry ent = (Map.Entry)itd3.next();
				if(ent.getKey().equals("agriculture"))
				{
					alph+='a';
				}
				else if(ent.getKey().equals("education"))
				{
					alph+='e';
				}
				else if (ent.getKey().equals("politics")) 
				{
					alph+='p';
				}
				else if(ent.getKey().equals("sports"))
				{
					alph+='s';
				}
				
			}
			aggrgt.put((String)entr1.getKey(), alph);
			alph="";
		}
		//System.out.println(aggrgt);
		
		Multimap<String, String> revmp = HashMultimap.create();
		for(Entry<String,String> entry : aggrgt.entrySet())
		{
			String val = entry.getValue();
			String tmp = "";
			
			for(int i=0; i<val.length(); i++)
			{
				if(val.charAt(i)=='a'){
					tmp+="agriculture , ";
				}
				else if (val.charAt(i)=='e') {
					tmp+="education , ";
				}
				else if (val.charAt(i)=='p') {
					tmp+="politics , ";
				}
				else if(val.charAt(i)=='s'){
					tmp+="sports , ";
				}
			}
			tmp = tmp.substring(0, tmp.length()-3);
			revmp.put(tmp, entry.getKey());
			
		}
		context.write(new Text("\n\n\nANSWER B - ii)"), new Text(" :"));
		context.write(new Text("\nStates having the same pattern of ranking of 4 words are : "), new Text(""));
		//System.out.println("\n\n\nANSWER B - ii) : ");
		//System.out.println("\nStates having the same pattern of ranking of 4 words are : ");

		for(String key : revmp.keySet()){
			context.write(new Text(key), new Text(revmp.get(key).toString()));
			//System.out.println(key + " = " + revmp.get(key));
		}
		
		
	}
	
}