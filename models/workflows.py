import uuid
from datetime import datetime
from typing import List

from sqlalchemy import Boolean, Column, DateTime, Enum, ForeignKey, JSON, String, Table
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, deferred, relationship

from models.base import Base

workflow_tree_mappings = Table(
    'workflow_tree_mappings', Base.metadata,
    Column("task_id", String(32), ForeignKey("workflow_tasks.id"), nullable=False),
    Column("next_task_id", String(32), ForeignKey("workflow_tasks.id"), nullable=False),
)


class WorkflowRoot(Base):
    """
    This database model holds the information for the root of the workflow tree
    """
    __tablename__ = 'workflow_roots'

    ID_KEY = "id"
    STATUS_KEY = "status"
    ASSOCIATED_TASKS_KEY = "associated_tasks"
    WORKFLOW_NAME_KEY = "workflow_name"
    WORKFLOW_NATURE_KEY = "workflow_nature"
    FE_REQUEST_DATA_KEY = "fe_request_data"
    CREATED_AT_KEY = "created_at"
    COMPLETED_AT_KEY = "completed_at"
    NEXT_ROOT_IDS_KEY = "next_root_ids"
    PREVIOUS_ROOT_IDS_KEY = "previous_root_ids"

    STATUS_PENDING = "PENDING"
    STATUS_INITIATED = "INITIATED"
    STATUS_RUNNING = "RUNNING"
    STATUS_C_SUCCESSFULLY = "COMPLETED_SUCCESSFULLY"
    STATUS_C_W_FAILURE = "COMPLETED_WITH_FAILURE"

    ALL_STATUSES_LIST = [
        STATUS_PENDING, STATUS_INITIATED, STATUS_RUNNING, STATUS_C_SUCCESSFULLY, STATUS_C_W_FAILURE
    ]

    ROOT_TYPE_NORMAL = "NORMAL"
    ROOT_TYPE_ON_SUCCESS = "ON_SUCCESS"
    ROOT_TYPE_ON_FAILURE = "ON_FAILURE"
    ROOT_TYPE_ON_COMPLETE = "ON_COMPLETE"
    ALL_ROOT_TYPES = \
        [ROOT_TYPE_NORMAL, ROOT_TYPE_ON_SUCCESS, ROOT_TYPE_ON_FAILURE, ROOT_TYPE_ON_COMPLETE]

    id = Column(String(32), primary_key=True)
    __status = Column("status", Enum(*ALL_STATUSES_LIST), default=STATUS_PENDING, nullable=False)
    workflow_name = Column(String(128))
    root_type = Column(Enum(*ALL_ROOT_TYPES), default=ROOT_TYPE_NORMAL)
    workflow_nature = Column(String(128))
    fe_request_data = deferred(Column(JSON))
    executor_running = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    initiated_at = Column(DateTime)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    user_id = Column(String(32), nullable=False)
    project_id = Column(String(32), nullable=False)
    __parent_root_copy = deferred(Column('parent_root_copy', JSON))

    parent_root_id = Column(String(32), ForeignKey('workflow_roots.id'))
    workflows_workspace_id = Column(String(32), ForeignKey('workflows_workspaces.id'))

    associated_tasks: ["WorkflowTask"] = \
        relationship('WorkflowTask', backref="root", cascade="all, delete-orphan", lazy='dynamic')

    def __init__(
            self, user_id, project_id, workflow_name=None, workflow_nature=None, fe_request_data=None,
            root_type=ROOT_TYPE_NORMAL
    ):
        self.id = str(uuid.uuid4().hex)
        self.created_at = datetime.utcnow()
        self.root_type = root_type or self.ROOT_TYPE_NORMAL
        self.status = self.STATUS_PENDING if self.ROOT_TYPE_NORMAL else self.STATUS_ON_HOLD
        self.workflow_name = workflow_name
        self.workflow_nature = workflow_nature
        self.fe_request_data = fe_request_data
        self.user_id = user_id
        self.project_id = project_id

    def add_next_task(self, next_task):
        """
        Add a task to the first group of tasks for the workflow
        :param next_task:
        :return:
        """
        assert isinstance(next_task, WorkflowTask)
        self.associated_tasks.append(next_task)

    @property
    def parent_root_copy(self):
        return self.__parent_root_copy

    def __generate_parent_root_copy(self):
        self.__parent_root_copy = self.parent_root.to_json()

    @hybrid_property
    def status(self):
        """
        Hybrid property (you can query on this) for status getter
        :return:
        """
        return self.__status

    @status.setter
    def status(self, new_status):
        """
        Hybrid property (you can query on this) for status setter
        :param new_status: <string> status to be set
        """
        if new_status in [self.STATUS_PENDING, self.STATUS_ON_HOLD]:
            if not self.created_at:
                self.created_at = datetime.utcnow()
        elif new_status == self.STATUS_INITIATED:
            self.initiated_at = datetime.utcnow()
        elif new_status == self.STATUS_RUNNING:
            self.started_at = datetime.utcnow()
        elif new_status in [
            self.STATUS_C_SUCCESSFULLY_WFC, self.STATUS_C_SUCCESSFULLY,
            self.STATUS_C_W_FAILURE
        ]:
            self.completed_at = datetime.utcnow()

        self.__status = new_status

    @property
    def next_roots(self):
        """
        Property to get next tasks associated with the current task
        :return:
        """
        return self._next_roots

    @property
    def previous_roots(self):
        """
        Property to get previous tasks associated with the current task
        :return:
        """
        return self._previous_roots

    @property
    def is_provisionable(self):
        """
        Property to get previous successful roots associated with the current root
        :return:
        """
        return self.previous_roots.filter(WorkflowRoot.status.in_(
            {WorkflowRoot.STATUS_C_SUCCESSFULLY}
        )).count() == self.previous_roots.count() and self.status not in [self.STATUS_C_SUCCESSFULLY,
                                                                          self.STATUS_RUNNING, self.STATUS_PENDING]

    @property
    def next_tasks(self):
        """
        Property to get next (first group) tasks of the root task
        :return:
        """
        return self.associated_tasks.filter(~WorkflowTask._previous_tasks.any()).all()

    @property
    def in_focus_tasks(self):
        """
        Property to get tasks which are in focus right now (running, failed, completed but not acknowledged, failed but
        not acknowledged)
        :return:
        """
        return self.associated_tasks.filter(WorkflowTask.in_focus).all()


class WorkflowTask(Base):
    """
    This database model holds information for a task that is tied to a resource
    """
    __tablename__ = 'workflow_tasks'

    ID_KEY = "id"
    STATUS_KEY = "status"
    RESOURCE_ID_KEY = "resource_id"
    RESOURCE_TYPE_KEY = "resource_type"
    TASK_TYPE_KEY = "task_type"
    TASK_METADATA_KEY = "task_metadata"
    RESULT_KEY = "result"
    MESSAGE_KEY = "message"
    IN_FOCUS_KEY = "in_focus"
    PREVIOUS_TASK_IDS_KEY = "previous_task_ids"
    NEXT_TASK_IDS_KEY = "next_task_ids"

    STATUS_PENDING = "PENDING"
    STATUS_INITIATED = "INITIATED"
    STATUS_RUNNING = "RUNNING"
    STATUS_RUNNING_WAIT = "RUNNING_WAIT"
    STATUS_RUNNING_WAIT_INITIATED = "RUNNING_WAIT_INITIATED"
    STATUS_SUCCESSFUL = "SUCCESSFUL"
    STATUS_FAILED = "FAILED"
    ALL_STATUSES_LIST = [
        STATUS_PENDING, STATUS_INITIATED, STATUS_RUNNING, STATUS_RUNNING_WAIT, STATUS_RUNNING_WAIT_INITIATED,
        STATUS_SUCCESSFUL, STATUS_FAILED
    ]

    TYPE_VALIDATE = "VALIDATE"
    TYPE_CREATE = "CREATE"
    TYPE_DELETE = "DELETE"
    TYPE_SYNC = "SYNC"
    TYPE_UPDATE = "UPDATE"

    id = Column(String(32), primary_key=True)
    resource_id = Column(String(32))
    resource_type = Column(String(512), nullable=False)
    task_type = Column(String(512), nullable=False)
    task_metadata = deferred(Column(JSON))
    result = deferred(Column(JSON))
    __status = Column("status", Enum(*ALL_STATUSES_LIST), default=STATUS_PENDING, nullable=False)
    message = Column(String(1024))
    in_focus = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    initiated_at = Column(DateTime)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    _next_tasks = relationship(
        'WorkflowTask',
        secondary=workflow_tree_mappings,
        primaryjoin=id == workflow_tree_mappings.c.task_id,
        secondaryjoin=id == workflow_tree_mappings.c.next_task_id,
        backref=backref('_previous_tasks', lazy='dynamic'),
        lazy='dynamic'
    )

    root_id = Column(String(32), ForeignKey('workflow_roots.id'), nullable=False)

    def __init__(self, task_type, resource_type, resource_id=None, task_metadata=None):
        self.id = str(uuid.uuid4().hex)
        self.resource_id = resource_id
        self.resource_type = resource_type
        self.task_type = task_type
        self.task_metadata = task_metadata
        self.created_at = datetime.utcnow()
        self.status = self.STATUS_PENDING

    def add_next_task(self, next_task):
        """
        Add subsequent task
        :param next_task: <object of WorkflowTask> Task that should run if this task is successful
        """
        assert isinstance(next_task, WorkflowTask)
        if not self.root:
            raise ValueError("Invalid operation add_next_task, {} does not have root".format(self.id))

        next_task.root = self.root
        self._next_tasks.append(next_task)

    def add_previous_task(self, previous_task):
        """
        Add pre-req task
        :param previous_task: <object of WorkflowTask> Task which should be successful for this task to run
        """
        assert isinstance(previous_task, WorkflowTask)
        if not self.root:
            raise ValueError("Invalid operation add_previous_task, {} does not have root".format(self.id))

        previous_task.root = self.root
        self._previous_tasks.append(previous_task)

    @hybrid_property
    def status(self):
        """
        Hybrid property (you can query on this) for status getter
        :return:
        """
        return self.__status

    @status.setter
    def status(self, new_status):
        """
        Hybrid property (you can query on this) for status setter
        :param new_status: <string> status to be set
        """
        if new_status == self.STATUS_INITIATED:
            self.initiated_at = datetime.utcnow()
        elif new_status == self.STATUS_RUNNING:
            self.started_at = datetime.utcnow()
        elif new_status == self.STATUS_SUCCESSFUL:
            self.completed_at = datetime.utcnow()
        elif new_status == WorkflowTask.STATUS_FAILED:
            self.completed_at = datetime.utcnow()

        self.__status = new_status

    @property
    def next_tasks(self) -> List["WorkflowTask"]:
        """
        Property to get next tasks associated with the current task
        :return:
        """
        return self._next_tasks

    @property
    def previous_tasks(self) -> List["WorkflowTask"]:
        """
        Property to get previous tasks associated with the current task
        :return:
        """
        return self._previous_tasks
