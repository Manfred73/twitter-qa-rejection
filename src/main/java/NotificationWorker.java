import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.client.ExternalTaskClient;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskService;

import java.util.Date;
import java.util.HashMap;

@Slf4j
public class NotificationWorker {

    public static void main(String[] args) {
        // bootstrap the client
        final var client = ExternalTaskClient.create()
                .baseUrl("http://localhost:8080/engine-rest")
                .asyncResponseTimeout(20000)
                .lockDuration(10000)
                .maxTasks(1)
                .build();

        // subscribe to the topic
        final var subscriptionBuilder = client.subscribe("notification");

        // handle job
        subscriptionBuilder.handler(NotificationWorker::handle);

        // release the subscription and start to work asynchronously on the tasks
        subscriptionBuilder.open();
    }

    private static void handle(ExternalTask externalTask, ExternalTaskService externalTaskService) {
        final var content = externalTask.getVariable("content");
        final var variables = new HashMap<String, Object>();
        variables.put("notficationTimestamp", new Date());

        if (!content.equals("Test")) {
            log.info("Sorry, your tweet has been rejected: " + content);
            externalTaskService.complete(externalTask, variables);
        } else {
            log.info("Failure, task unlocked");
            externalTaskService.unlock(externalTask);
            externalTaskService.complete(externalTask, variables);
        }
    }
}
