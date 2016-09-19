package wordcount;
/**
 * wordcount.MrBatchApp
 *
 * @author lujin
 * @date 16/9/18
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MrBatchApp {

    private static final Log log = LogFactory.getLog(MrBatchApp.class);

    public static void main(String[] args) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        System.out.println("[INFO] start Hadoop batch job ...");

        AbstractApplicationContext context = new ClassPathXmlApplicationContext("classpath:/META-INF/spring/*-context.xml");
        log.info("Batch Tweet wordcount MR Job Running");
        context.registerShutdownHook();

        JobLauncher jobLauncher = context.getBean(JobLauncher.class);
        Job job = context.getBean(Job.class);
        jobLauncher.run(job, new JobParameters());

        System.out.println("[NOTICE] Hadoop job is done");

    }
}