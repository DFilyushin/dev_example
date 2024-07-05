from typing import Iterable, Optional, List
from datetime import datetime

from bson.objectid import ObjectId
from pymongo import ASCENDING

from connection_managers.mongo_connection_manager import MongoConnectionManager
from models.task_model import TaskModel, TaskStatusEnum, ExternalServiceEnum
from repositories.base_repository import BaseRepository, IndexDef
from settings import Settings


class TaskRepository(BaseRepository):

    def __init__(self, connection_manager: MongoConnectionManager, settings: Settings):
        super().__init__(connection_manager, settings)

    @property
    def collection_name(self) -> str:
        return 'task'

    @property
    def collection_indexes(self) -> Iterable[IndexDef]:
        return (
            IndexDef(name='client_uid', sort=ASCENDING),
            IndexDef(name='creation_date', sort=ASCENDING),
            IndexDef(name='forced_date', sort=ASCENDING),
            IndexDef(name='external_task_id', sort=ASCENDING),
        )

    async def save_task(self, task: TaskModel) -> str:
        task_id = await self.save_data(task.dict())
        return str(task_id)

    async def get_task_by_id(self, task_id: str) -> Optional[TaskModel]:
        criteria = {'_id': ObjectId(task_id)}
        result = await self.get_data(criteria)
        if result:
            return TaskModel(**result)

    async def get_active_tasks(self, limit: int, offset: int) -> List[TaskModel]:
        """Список активных задач"""
        criteria = {'task_status': {"$in": [TaskStatusEnum.NEW.value, TaskStatusEnum.PROCESSING.value]}}
        result = await self.get_list_data(criteria=criteria, limit=limit, skip=offset)
        return [TaskModel(**item) for item in result]

    async def update_task(self, task_id: str, replacement_data: dict) -> None:
        await self.update_data({'_id': ObjectId(task_id)}, replacement_data)

    async def check_last_existing_task_by_client_uid(self, client_uid: str) -> dict:
        criteria = {'client_uid': client_uid}
        sort_criteria = [('modification_date', -1)]
        tasks = await self.get_list_data(criteria, sort_criteria, limit=1)
        if tasks:
            return tasks[0]

    async def add_external_service_error(self, task_id: str, service_name: ExternalServiceEnum):
        criteria = {'_id': ObjectId(task_id)}
        await self.add_nested_item(criteria, {
            'external_service_error_history': {
                'service_name': service_name,
                'event_date': datetime.utcnow()
            }})
