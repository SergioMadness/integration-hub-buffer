<?php namespace professionalweb\IntegrationHub\IntegrationHubBuffer\Console\Command;

use Illuminate\Console\Command;
use Illuminate\Foundation\Bus\DispatchesJobs;
use professionalweb\IntegrationHub\IntegrationHubCommon\Jobs\NewRequest;
use professionalweb\IntegrationHub\IntegrationHubCommon\Interfaces\EventData;
use professionalweb\IntegrationHub\IntegrationHubCommon\Interfaces\Repositories\RequestRepository;

class EventSelector extends Command
{
    use DispatchesJobs;

    public function handle(RequestRepository $repository): void
    {
        $offset = 0;
        $offsetNum = 100;
        do {
            $collection = $repository->get(['status' => EventData::STATUS_RETRY], ['id' => 'asc'], $offsetNum, $offset);
            $collection->each(function (EventData $model) {
                $this->dispatch(
                    (new NewRequest($model))
                        ->onConnection(config('integration-hub.new-event-connection'))
                        ->onQueue(config('integration-hub.new-event-queue'))
                );
            });
            $offset += $offsetNum;
        } while ($collection->isNotEmpty());
    }
}