package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.NodeUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.SourceException;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.ddl.DDLFilter;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.TableMonitor;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:59
 **/
public abstract class HazelcastSourcePdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	public static final long PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT = 10L;
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkBaseNode.class);
	protected SyncProgress syncProgress;
	protected ExecutorService sourceRunner = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
	protected ScheduledExecutorService tableMonitorResultHandler;
	protected SnapshotProgressManager snapshotProgressManager;
	protected LinkedBlockingQueue<TapdataEvent> eventQueue = new LinkedBlockingQueue<>(10);
	private TapdataEvent pendingEvent;
	protected SourceMode sourceMode = SourceMode.NORMAL;
	protected Long initialFirstStartTime = System.currentTimeMillis();
	protected TransformerWsMessageDto transformerWsMessageDto;
	protected DDLFilter ddlFilter;
	protected ReentrantLock sourceRunnerLock;
	protected AtomicBoolean endSnapshotLoop;
	protected CopyOnWriteArrayList<String> newTables;
	protected CopyOnWriteArrayList<String> removeTables;
	protected AtomicBoolean sourceRunnerFirstTime;
	private DAGDataServiceImpl dagDataService;

	public HazelcastSourcePdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		if (!StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getSubTaskDto().getParentTask().getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			initMilestoneService(MilestoneContext.VertexType.SOURCE);
		}
		// MILESTONE-INIT_CONNECTOR-RUNNING
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.RUNNING);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		try {
			createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
			connectorNodeInit(dataProcessorContext);
		} catch (Throwable e) {
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw new RuntimeException(e);
		}
		SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
		syncProgress = initSyncProgress(subTaskDto.getAttrs());
		if (!StringUtils.equalsAnyIgnoreCase(subTaskDto.getParentTask().getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			initBatchAndStreamOffset(subTaskDto);
		}
		initDDLFilter();
		this.sourceRunnerLock = new ReentrantLock();
		this.endSnapshotLoop = new AtomicBoolean(false);
		this.transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
				ConnectorConstant.TASK_COLLECTION + "/transformParam/" + processorBaseContext.getSubTaskDto().getParentTask().getId().toHexString(),
				TransformerWsMessageDto.class);
		this.sourceRunnerFirstTime = new AtomicBoolean(true);
		this.sourceRunner.submit(this::startSourceRunner);
		initTableMonitor();
	}

	private void initDDLFilter() {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			Boolean enableDDL = ((DataParentNode<?>) node).getEnableDDL();
			List<String> disabledEvents = ((DataParentNode<?>) node).getDisabledEvents();
			this.ddlFilter = DDLFilter.create(enableDDL, disabledEvents).dynamicTableTest(this::needDynamicTable);
		}
	}

	private void initTableMonitor() throws Exception {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			if (needDynamicTable(null)) {
				TableMonitor tableMonitor = new TableMonitor(dataProcessorContext.getTapTableMap(), associateId, dataProcessorContext.getSubTaskDto());
				this.monitorManager.startMonitor(tableMonitor);
				this.tableMonitorResultHandler = new ScheduledThreadPoolExecutor(1);
				this.tableMonitorResultHandler.scheduleAtFixedRate(this::handleTableMonitorResult, 0L, PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT, TimeUnit.SECONDS);
				logger.info("Handle dynamic add/remove table thread started, interval: "+PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT+" seconds");
				this.newTables = new CopyOnWriteArrayList<>();
				this.removeTables = new CopyOnWriteArrayList<>();
			}
		}
	}

	private boolean needDynamicTable(Object obj) {
		Node<?> node = dataProcessorContext.getNode();
		if (node instanceof DatabaseNode) {
			Boolean enableDynamicTable = ((DatabaseNode) node).getEnableDynamicTable();
			if (null == enableDynamicTable || !enableDynamicTable) {
				return false;
			}
			if (syncType.equals(SyncTypeEnum.INITIAL_SYNC)) {
				return false;
			}
			GetTableNamesFunction getTableNamesFunction = getConnectorNode().getConnectorFunctions().getGetTableNamesFunction();
			if (null == getTableNamesFunction) {
				return false;
			}
		} else {
			return false;
		}
		return true;
	}

	private void initBatchAndStreamOffset(SubTaskDto subTaskDto) {
		if (syncProgress == null) {
			syncProgress = new SyncProgress();
			syncProgress.setBatchOffsetObj(new HashMap<>());
			// null present current
			Long offsetStartTimeMs = null;
			switch (syncType) {
				case INITIAL_SYNC_CDC:
					initStreamOffsetFromTime(offsetStartTimeMs);
					break;
				case INITIAL_SYNC:
					syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
					break;
				case CDC:
					List<TaskDto.SyncPoint> syncPoints = subTaskDto.getParentTask().getSyncPoints();
					String connectionId = NodeUtil.getConnectionId(dataProcessorContext.getNode());
					TaskDto.SyncPoint syncPoint = null;
					if (null != syncPoints) {
						syncPoint = syncPoints.stream().filter(sp -> sp.getConnectionId().equals(connectionId)).findFirst().orElse(null);
					}
					String pointType = syncPoint == null ? "current" : syncPoint.getPointType();
					if (StringUtils.isBlank(pointType)) {
						throw new RuntimeException("Run cdc task failed, sync point type cannot be empty");
					}
					switch (pointType) {
						case "localTZ":
						case "connTZ":
							// todo missing db timezone
							offsetStartTimeMs = syncPoint.getDateTime();
							break;
						case "current":
							break;
					}
					initStreamOffsetFromTime(offsetStartTimeMs);
					break;
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				TapdataEvent tapdataEvent = new TapdataHeartbeatEvent(offsetStartTimeMs, syncProgress.getStreamOffsetObj());
				enqueue(tapdataEvent);
			}
		} else {
			String batchOffset = syncProgress.getBatchOffset();
			if (StringUtils.isNotBlank(batchOffset)) {
				syncProgress.setBatchOffsetObj(PdkUtil.decodeOffset(batchOffset, getConnectorNode()));
			} else {
				syncProgress.setBatchOffsetObj(new HashMap<>());
			}
			String streamOffset = syncProgress.getStreamOffset();
			SyncProgress.Type type = syncProgress.getType();
			switch (type) {
				case NORMAL:
					if (StringUtils.isNotBlank(streamOffset)) {
						syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
					} else {
						initStreamOffsetFromTime(null);
					}
					break;
				case SHARE_CDC:
					if (((DataProcessorContext) processorBaseContext).getSourceConn().isShareCdcEnable()
							&& processorBaseContext.getSubTaskDto().getParentTask().getShareCdcEnable()) {
						// continue cdc from share log storage
						if (StringUtils.isNotBlank(streamOffset)) {
							syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
						} else {
							initStreamOffsetFromTime(null);
						}
					} else {
						// switch share cdc to normal task
						Long eventTime = syncProgress.getEventTime();
						if (null == eventTime) {
							throw new RuntimeException("It was found that the task was switched from shared incremental to normal mode and cannot continue execution, reason: lost breakpoint timestamp."
									+ " Please try to reset and start the task.");
						}
						initStreamOffsetFromTime(eventTime);
					}
					break;
			}
		}
	}

	private void initStreamOffsetFromTime(Long offsetStartTimeMs) {
		AtomicReference<Object> timeToStreamOffsetResult = new AtomicReference<>();
		TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = getConnectorNode().getConnectorFunctions().getTimestampToStreamOffsetFunction();
		if (null != timestampToStreamOffsetFunction) {
			PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TIMESTAMP_TO_STREAM_OFFSET, () -> {
				try {
					timeToStreamOffsetResult.set(timestampToStreamOffsetFunction.timestampToStreamOffset(getConnectorNode().getConnectorContext(), offsetStartTimeMs));
				} catch (Throwable e) {
					if (need2InitialSync(syncProgress)) {
						logger.warn("Call timestamp to stream offset function failed, will stop task after snapshot, type: " + dataProcessorContext.getDatabaseType()
								+ ", errors: " + e.getClass().getSimpleName() + "  " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					} else {
						throw new RuntimeException("Call timestamp to stream offset function failed, will stop task, type: " + dataProcessorContext.getDatabaseType()
								+ ", errors: " + e.getClass().getSimpleName() + "  " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					}
				}
				syncProgress.setStreamOffsetObj(timeToStreamOffsetResult.get());
			}, TAG);
		} else {
			logger.warn("Pdk connector does not support timestamp to stream offset function, will stop task after snapshot: " + dataProcessorContext.getDatabaseType());
		}
	}

	@Override
	final public boolean complete() {
		try {
			SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
			Log4jUtil.setThreadContext(subTaskDto);
			TapdataEvent dataEvent;
			AtomicBoolean isPending = new AtomicBoolean();
			if (!isRunning()) {
				return true;
			}
			if (pendingEvent != null) {
				dataEvent = pendingEvent;
				pendingEvent = null;
				isPending.compareAndSet(false, true);
			} else {
				dataEvent = eventQueue.poll(5, TimeUnit.SECONDS);
				isPending.compareAndSet(true, false);
			}

			if (dataEvent != null) {
				TapEvent tapEvent;
				if (!isPending.get()) {
					TapCodecsFilterManager codecsFilterManager = getConnectorNode().getCodecsFilterManager();
					tapEvent = dataEvent.getTapEvent();
					if (sourceMode == SourceMode.NORMAL) {
						tapRecordToTapValue(tapEvent, codecsFilterManager);
					}
				}
				if (!offer(dataEvent)) {
					pendingEvent = dataEvent;
					return false;
				}
				Optional.ofNullable(snapshotProgressManager)
						.ifPresent(s -> s.incrementEdgeFinishNumber(TapEventUtil.getTableId(dataEvent.getTapEvent())));
			}
			if (error != null) {
				throw new RuntimeException(error);
			}
		} catch (Exception e) {
			logger.error("Source sync failed {}.", e.getMessage(), e);
			throw new SourceException(e, true);
		}

		return false;
	}

	private void handleTableMonitorResult() {
		Thread.currentThread().setName("Handle-Table-Monitor-Result-" + this.associateId);
		Log4jUtil.setThreadContext(dataProcessorContext.getSubTaskDto());
		// Handle dynamic table change
		Object tableMonitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.TABLE_MONITOR);
		if (tableMonitor instanceof TableMonitor) {
			((TableMonitor) tableMonitor).consume(tableResult -> {
				List<String> addList = tableResult.getAddList();
				List<String> removeList = tableResult.getRemoveList();
				if (CollectionUtils.isNotEmpty(addList) || CollectionUtils.isNotEmpty(removeList)) {
					try {
						while (isRunning()) {
							try {
								if (sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)) {
									break;
								}
							} catch (InterruptedException e) {
								break;
							}
						}
						// Handle new table(s)
						if (CollectionUtils.isNotEmpty(addList)) {
							logger.info("Found new table(s): " + addList);
							List<TapTable> addTapTables = new ArrayList<>();
							List<TapdataEvent> tapdataEvents = new ArrayList<>();
							// Load schema
							LoadSchemaRunner.pdkDiscoverSchema(getConnectorNode(), addList, addTapTables::add);
							logger.info("Load new table's schema finished");
							if (CollectionUtils.isEmpty(addTapTables)) {
								String error = "Load new table schema failed, expect table count: " + addList.size() + ", actual: 0";
								errorHandle(new RuntimeException(error), error);
							}
							if (addList.size() != addTapTables.size()) {
								String error = "Load new table schema failed, expect table count: " + addList.size() + ", actual: " + addTapTables.size();
								errorHandle(new RuntimeException(error), error);
							}
							for (TapTable addTapTable : addTapTables) {
								if (!isRunning()) {
									break;
								}
								TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
								tapCreateTableEvent.table(addTapTable);
								tapCreateTableEvent.setTableId(addTapTable.getId());
								TapdataEvent tapdataEvent = wrapTapdataEvent(tapCreateTableEvent, SyncStage.valueOf(syncProgress.getSyncStage()), null, false);
								if (null == tapdataEvent) {
									String error = "Wrap create table tapdata event failed: " + addTapTable;
									errorHandle(new RuntimeException(error), error);
									return;
								}
								tapdataEvents.add(tapdataEvent);
							}
							if (!isRunning()) {
								return;
							}
							tapdataEvents.forEach(this::enqueue);
							this.newTables.addAll(addList);
							if (this.endSnapshotLoop.get()) {
								logger.info("It is detected that the snapshot reading has ended, and the reading thread will be restarted");
								// Restart source runner
								if (null != sourceRunner) {
									this.sourceRunnerFirstTime.set(false);
									if (null != getConnectorNode()) {
										PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.STOP, () -> getConnectorNode().connectorStop(), TAG);
										PDKIntegration.releaseAssociateId(this.associateId);
										ConnectorNodeService.getInstance().removeConnectorNode(this.associateId);
										createPdkConnectorNode(dataProcessorContext, jetContext.hazelcastInstance());
										connectorNodeInit(dataProcessorContext);
									} else {
										String error = "Connector node is null";
										errorHandle(new RuntimeException(error), error);
										return;
									}
									this.sourceRunner.shutdownNow();
									this.sourceRunner = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
									sourceRunner.submit(this::startSourceRunner);
								} else {
									String error = "Source runner is null";
									errorHandle(new RuntimeException(error), error);
									return;
								}
							}
						}
						// Handle remove table(s)
						if (CollectionUtils.isNotEmpty(removeList)) {
							logger.info("Found remove table(s): " + removeList);
							List<TapdataEvent> tapdataEvents = new ArrayList<>();
							for (String tableName : removeList) {
								if (!isRunning()) {
									break;
								}
								TapDropTableEvent tapDropTableEvent = new TapDropTableEvent();
								tapDropTableEvent.setTableId(tableName);
								TapdataEvent tapdataEvent = wrapTapdataEvent(tapDropTableEvent, SyncStage.valueOf(syncProgress.getSyncStage()), null, false);
								tapdataEvents.add(tapdataEvent);
							}
							tapdataEvents.forEach(this::enqueue);
						}
					} catch (Throwable throwable) {
						String error = "Handle table monitor result failed, result: " + tableResult + ", error: " + throwable.getMessage();
						errorHandle(new RuntimeException(error, throwable), error);
					} finally {
						if (sourceRunnerLock.isLocked()) {
							sourceRunnerLock.unlock();
						}
					}
				}
			});
		}
	}

	abstract void startSourceRunner();

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events) {
		return wrapTapdataEvent(events, SyncStage.INITIAL_SYNC, null);
	}

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events, SyncStage syncStage, Object offsetObj) {
		List<TapdataEvent> tapdataEvents = new ArrayList<>(events.size() + 1);
		for (int i = 0; i < events.size(); i++) {
			TapEvent tapEvent = events.get(i);
			boolean isLast = i == (events.size() - 1);
			TapdataEvent tapdataEvent = wrapTapdataEvent(tapEvent, syncStage, offsetObj, isLast);
			if (null == tapdataEvent) {
				continue;
			}
			tapdataEvents.add(tapdataEvent);
		}
		return tapdataEvents;
	}

	protected TapdataEvent wrapTapdataEvent(TapEvent tapEvent, SyncStage syncStage, Object offsetObj, boolean isLast) {
		TapdataEvent tapdataEvent = null;
		if (tapEvent instanceof TapRecordEvent) {
			TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
			switch (sourceMode) {
				case NORMAL:
					tapdataEvent = new TapdataEvent();
					break;
				case SHARE_CDC:
					tapdataEvent = new TapdataShareLogEvent();
					break;
			}
			tapdataEvent.setTapEvent(tapRecordEvent);
			tapdataEvent.setSyncStage(syncStage);
			if (SyncStage.INITIAL_SYNC == syncStage) {
				if (isLast && !StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getSubTaskDto().getParentTask().getSyncType(),
						TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
					Map<String, Object> batchOffsetObj = (Map<String, Object>) syncProgress.getBatchOffsetObj();
					Map<String, Object> newMap = new HashMap<>();
					try {
						MapUtil.deepCloneMap(batchOffsetObj, newMap);
					} catch (IllegalAccessException | InstantiationException e) {
						throw new RuntimeException("Deep clone batch offset map failed: " + e.getMessage(), e);
					}
					tapdataEvent.setBatchOffset(newMap);
				}
			} else if (SyncStage.CDC == syncStage) {
				tapdataEvent.setStreamOffset(offsetObj);
				if (null == ((TapRecordEvent) tapEvent).getReferenceTime())
					throw new RuntimeException("Tap CDC event's reference time is null");
				tapdataEvent.setSourceTime(((TapRecordEvent) tapEvent).getReferenceTime());
			}
		} else if (tapEvent instanceof HeartbeatEvent) {
			tapdataEvent = new TapdataHeartbeatEvent(((HeartbeatEvent) tapEvent).getReferenceTime(), offsetObj);
		} else if (tapEvent instanceof TapDDLEvent) {
			if (null != ddlFilter && !ddlFilter.test((TapDDLEvent) tapEvent)) {
				logger.warn("DDL events are filtered: " + tapEvent);
				return null;
			}
			tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapEvent);
			tapdataEvent.setSyncStage(syncStage);
			tapdataEvent.setStreamOffset(offsetObj);
			tapdataEvent.setSourceTime(((TapDDLEvent) tapEvent).getReferenceTime());
			String tableId = ((TapDDLEvent) tapEvent).getTableId();
			TapTable tapTable;
			// Modify schema by ddl event
			if (tapEvent instanceof TapCreateTableEvent) {
				tapTable = ((TapCreateTableEvent) tapEvent).getTable();
			} else {
				try {
					tapTable = processorBaseContext.getTapTableMap().get(tableId);
					InstanceFactory.bean(DDLSchemaHandler.class).updateSchemaByDDLEvent((TapDDLEvent) tapEvent, tapTable);
					TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
					DefaultExpressionMatchingMap dataTypesMap = getConnectorNode().getConnectorContext().getSpecification().getDataTypesMap();
					tableFieldTypesGenerator.autoFill(tapTable.getNameFieldMap(), dataTypesMap);
				} catch (Exception e) {
					RuntimeException runtimeException = new RuntimeException("Modify schema by ddl failed, ddl type: " + tapEvent.getClass() + ", error: " + e.getMessage(), e);
					errorHandle(runtimeException, runtimeException.getMessage());
					throw runtimeException;
				}
			}

			// Refresh task config by ddl event
			DAG dag = processorBaseContext.getSubTaskDto().getDag();
			try {
				// Update DAG config
				dag.filedDdlEvent(processorBaseContext.getNode().getId(), (TapDDLEvent) tapEvent);
				DAG cloneDag = dag.clone();
				// Put new DAG into info map
				tapEvent.addInfo(NEW_DAG_INFO_KEY, cloneDag);
			} catch (Exception e) {
				RuntimeException runtimeException = new RuntimeException("Update DAG by TapDDLEvent failed, error: " + e.getMessage(), e);
				errorHandle(runtimeException, runtimeException.getMessage());
				throw runtimeException;
			}
			// Refresh task schema by ddl event
			try {
				List<MetadataInstancesDto> insertMetadata = new CopyOnWriteArrayList<>();
				Map<String, MetadataInstancesDto> updateMetadata = new ConcurrentHashMap<>();
				List<String> removeMetadata = new CopyOnWriteArrayList<>();
				if (null == transformerWsMessageDto) {
					transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
							ConnectorConstant.TASK_COLLECTION + "/transformParam/" + processorBaseContext.getSubTaskDto().getParentTask().getId().toHexString(),
							TransformerWsMessageDto.class);
				}
				List<MetadataInstancesDto> metadataInstancesDtoList = transformerWsMessageDto.getMetadataInstancesDtoList();
				Map<String, String> qualifiedNameIdMap = metadataInstancesDtoList.stream()
						.collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m.getId().toHexString()));
				tapEvent.addInfo(QUALIFIED_NAME_ID_MAP_INFO_KEY, qualifiedNameIdMap);
				if (null == dagDataService) {
					dagDataService = new DAGDataServiceImpl(transformerWsMessageDto);
				}
				String qualifiedName = "";
				Map<String, List<Message>> errorMessage;
				if (tapEvent instanceof TapCreateTableEvent) {
					qualifiedName = dagDataService.createNewTable(dataProcessorContext.getSourceConn().getId(), tapTable);
					logger.info("Create new table in memory, qualified name: " + qualifiedName);
					dataProcessorContext.getTapTableMap().putNew(tapTable.getId(), tapTable, qualifiedName);
					errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
					MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
					insertMetadata.add(metadata);
					logger.info("Create new table schema transform finished: " + tapTable);
				} else if (tapEvent instanceof TapDropTableEvent) {
					qualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(((TapDropTableEvent) tapEvent).getTableId());
					logger.info("Drop table in memory qualified name: " + qualifiedName);
					dagDataService.dropTable(qualifiedName);
					errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
					dataProcessorContext.getTapTableMap().remove(tapTable.getId());
					removeMetadata.add(qualifiedName);
					logger.info("Drop table schema transform finished");
				} else {
					qualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(tableId);
					logger.info("Alter table in memory, qualified name: " + qualifiedName);
					dagDataService.coverMetaDataByTapTable(qualifiedName, tapTable);
					errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
					MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
					updateMetadata.put(metadata.getId().toHexString(), metadata);
					logger.info("Alter table schema transform finished");
				}
				tapEvent.addInfo(INSERT_METADATA_INFO_KEY, insertMetadata);
				tapEvent.addInfo(UPDATE_METADATA_INFO_KEY, updateMetadata);
				tapEvent.addInfo(REMOVE_METADATA_INFO_KEY, removeMetadata);
				tapEvent.addInfo(DAG_DATA_SERVICE_INFO_KEY, dagDataService);
				tapEvent.addInfo(TRANSFORM_SCHEMA_ERROR_MESSAGE_INFO_KEY, errorMessage);
			} catch (Exception e) {
				RuntimeException runtimeException = new RuntimeException("Transform schema by TapDDLEvent failed, error: " + e.getMessage(), e);
				errorHandle(runtimeException, runtimeException.getMessage());
				throw runtimeException;
			}
		}
		if (null == tapdataEvent) {
			RuntimeException runtimeException = new RuntimeException("Found event type does not support: " + tapEvent.getClass().getSimpleName());
			errorHandle(runtimeException, runtimeException.getMessage());
			throw runtimeException;
		}
		return tapdataEvent;
	}

	protected void enqueue(TapdataEvent tapdataEvent) {
		while (isRunning()) {
			try {
				if (eventQueue.offer(tapdataEvent, 3, TimeUnit.SECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	@Override
	public void doClose() throws Exception {
		try {
			Optional.ofNullable(tableMonitorResultHandler).ifPresent(ExecutorService::shutdownNow);
			Optional.ofNullable(sourceRunner).ifPresent(ExecutorService::shutdownNow);
		} finally {
			super.doClose();
		}
	}

	public LinkedBlockingQueue<TapdataEvent> getEventQueue() {
		return eventQueue;
	}

	public SnapshotProgressManager getSnapshotProgressManager() {
		return snapshotProgressManager;
	}

	public enum SourceMode {
		NORMAL,
		SHARE_CDC,
	}
}