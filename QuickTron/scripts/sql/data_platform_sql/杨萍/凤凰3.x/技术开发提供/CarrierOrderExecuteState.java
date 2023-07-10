package com.kc.phoenix.rss.carrier.core.enums;

import com.google.common.collect.Lists;
import com.kc.phoenix.common.utils.StreamUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 潜伏式作业单执行状态
 *
 * @author guzhixiang
 * @date 2022/5/24
 */
public enum CarrierOrderExecuteState {
    /**
     * 未执行
     */
    INIT(0, "INIT", "未执行"),
    /**
     * 开始
     */
    START(0, "START", "开始"),
    /**
     * 待执行
     */
    WAITING_NEXTSTOP(10, "WAITING_NEXTSTOP", "待执行"),
    /**
     * 待分配ROBOT
     */
    WAITING_ROBOT(20, "WAITING_ROBOT", "待分配ROBOT"),
    /**
     * 待下发
     */
    WAITING_RESOURCE(22, "WAITING_RESOURCE", "待下发"),
    /**
     * 待下发(多楼层)
     */
    PENDING_INIT_JOB(23, "PENDING_INIT_JOB", "待下发(多楼层)"),
    /**
     * 等待任务下发
     */
    WAITING_DISPATCHER(25, "WAITING_DISPACHER", "等待任务下发"),
    /**
     * 已分配
     */
    INIT_JOB(30, "INIT_JOB", "已分配"),
    /**
     * 执行中
     */
    EXECUTING(35, "EXECUTING", "执行中"),
    /**
     * 开始空车移动（潜伏式）
     */
    MOVE_START(36, "MOVE_START", "开始空车移动"),
    /**
     * 空车移动结束（潜伏式）
     */
    MOVE_DONE(37, "MOVE_DONE", "空车移动结束"),
    /**
     * 开始顶升
     */
    LIFT_UP_START(40, "LIFT_UP_START", "开始顶升"),
    /**
     * 顶升完成
     */
    LIFT_UP_DONE(45, "LIFT_UP_DONE", "顶升完成"),
    /**
     * 带载开始移动(潜伏式)
     */
    RACK_MOVE_START(55, "RACK_MOVE_START", "带载开始移动"),
    /**
     * 带载开始结束(潜伏式)
     */
    RACK_MOVE_DONE(56, "RACK_MOVE_DONE", "带载开始结束"),
    /**
     * 急停
     */
    SUSPEND(60, "SUSPEND", "急停"),
    /**
     * 开始放下
     */
    PUT_DOWN_START(70, "PUT_DOWN_START", "开始放下货架"),
    /**
     * 放下货架完成
     */
    PUT_DOWN_DONE(75, "PUT_DOWN_DONE", "放下货架完成"),
    /**
     * 到站
     */
    ENTER_STATION(80, "ENTER_STATION", "到站"),
    /**
     * 开始二次移动
     */
    AGAIN_MOVE_START(84, "AGAIN_MOVE_START", "开始二次移动"),
    /**
     * 二次移动完成
     */
    AGAIN_MOVE_DONE(85, "AGAIN_MOVE_DONE", "二次移动完成"),
    /**
     * 完成
     */
    DONE(90, "DONE", "完成"),
    /**
     * 挂起
     */
    PENDING(100, "PENDING", "挂起"),
    /**
     * 取消
     */
    CANCELED(110, "CANCEL", "取消"),
    /**
     * 失败
     */
    FAILED(120, "FAILED", "失败"),
    /**
     * 异常完成
     */
    ABNORMAL_COMPLETED(130, "ABNORMAL_COMPLETED", "异常完成"),

    /**
     * 等待分配电梯
     */
    WAITING_LIFT(140, "WAITING_LIFT", "等待分配电梯"),
    /**
     * 异常取消
     */
    ABNORMAL_CANCEL(150, "ABNORMAL_CANCEL", "异常取消");


    private int code;
    private String enDesc;
    private String desc;

    CarrierOrderExecuteState(int code, String enDesc, String desc) {
        this.code = code;
        this.enDesc = enDesc;
        this.desc = desc;

    }

    public String getDesc() {
        return this.desc;
    }

    public String getEnDesc() {
        return this.enDesc;
    }

    public int getCode() {
        return this.code;
    }

    public static List<CarrierOrderExecuteState> getAllStates() {
        return ALL_STATES;
    }

    /**
     * 获取电梯能感知的状态， 需要跨楼层的任务，当第一段robot任务下发rcs后robotJob变成INIT_JOB，
     * 直到该搬运任务结束前会一直停留在这个状态，电梯的状态不同步回robotJob
     *
     * @return
     */
    public static List<String> getLiftAwareState() {
        /**
         * 对于电梯，
         */
        return Arrays.asList(WAITING_ROBOT, WAITING_LIFT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START,
                        LIFT_UP_DONE, RACK_MOVE_START,
                        RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE, AGAIN_MOVE_START, AGAIN_MOVE_DONE)
                .stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<CarrierOrderExecuteState> getWorkingStates() {
        return Lists.newArrayList(
                WAITING_ROBOT, WAITING_LIFT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE, ENTER_STATION, AGAIN_MOVE_START, AGAIN_MOVE_DONE);
    }

    public static List<String> workingState() {
        return Arrays.asList(INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                        RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE, WAITING_DISPATCHER, ENTER_STATION,
                         AGAIN_MOVE_START, AGAIN_MOVE_DONE)
                .stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<String> getNotScheduledState() {
        return Arrays.asList(WAITING_NEXTSTOP, WAITING_ROBOT, INIT, WAITING_RESOURCE, PENDING_INIT_JOB,
                WAITING_DISPATCHER).stream().map(Enum::name).collect(Collectors.toList());
    }

    public static List<String> getScheduledState() {
        return Arrays.asList(INIT_JOB, EXECUTING, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                        RACK_MOVE_DONE, SUSPEND, PUT_DOWN_START, PUT_DOWN_DONE,
                        ENTER_STATION, DONE, PENDING, CANCELED, FAILED, ABNORMAL_COMPLETED, WAITING_LIFT,
                        ABNORMAL_CANCEL, AGAIN_MOVE_START, AGAIN_MOVE_DONE)
                .stream().map(Enum::name).collect(Collectors.toList());
    }

    /**
     * 未完成状态
     */
    public static List<CarrierOrderExecuteState> getNotDoneStates() {
        return Arrays.stream(values()).collect(Collectors.toList()).stream().filter(state ->
                !getDoneStates().contains(state)).collect(Collectors.toList());
    }

    public static List<CarrierOrderExecuteState> getDoneStates() {
        return Lists.newArrayList(DONE, ENTER_STATION, CANCELED, ABNORMAL_COMPLETED, ABNORMAL_CANCEL);
    }

    public static List<CarrierOrderExecuteState> getDoneStatesV2() {
        return Lists.newArrayList(DONE, CANCELED, FAILED, ABNORMAL_COMPLETED, ABNORMAL_CANCEL);
    }

    public static List<String> getAbnormalStates() {
        List<String> states = Lists.newArrayList(
                CANCELED,
                ABNORMAL_COMPLETED,
                ABNORMAL_CANCEL).stream().map(Enum::name).collect(Collectors.toList());
        return states;
    }

    public static List<CarrierOrderExecuteState> occupyPointState() {
        return Lists.newArrayList(
                WAITING_ROBOT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START,
                RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE, ENTER_STATION);
    }

    /**
     * 可以取消的任务状态
     */
    public static List<CarrierOrderExecuteState> getCancelAbleStates() {
        return Lists.newArrayList(INIT, WAITING_NEXTSTOP, WAITING_ROBOT, INIT_JOB, MOVE_START, MOVE_DONE, LIFT_UP_START,
                LIFT_UP_DONE, SUSPEND, RACK_MOVE_START, RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE,
                ENTER_STATION, PENDING);

    }

    /**
     * 不可以急停的任务状态
     */
    public static List<CarrierOrderExecuteState> getNotSuspendStates() {
        return Lists.newArrayList(CANCELED, ENTER_STATION, SUSPEND, DONE, FAILED, ABNORMAL_COMPLETED, ABNORMAL_CANCEL, PENDING);
    }

    /**
     * 已完结状态
     */
    public static List<String> getCompletedStates() {
        return Lists.newArrayList(CANCELED.name(),
                DONE.name(),
                FAILED.name(),
                ABNORMAL_COMPLETED.name(),
                ABNORMAL_CANCEL.name());
    }




    /**
     * 待执行的任务
     *
     * @return
     */
    public static List<CarrierOrderExecuteState> getPendingStates() {
        return Lists.newArrayList(WAITING_NEXTSTOP, INIT);
    }

    private static final List<CarrierOrderExecuteState> ALL_STATES;
    private static final List<String> CONSOLE_DONE_STATE;
    private static final List<String> CONSOLE_EXCEPTION_STATE;
    private static final List<String> CONSOLE_EXECUTING_STATE;
    private static final List<String> CONSOLE_WAITING_STATE;

    private static List<String> toArray(CarrierOrderExecuteState... states) {
        return StreamUtils.mapToList(Arrays.asList(states), CarrierOrderExecuteState::name);
    }

    static {
        ALL_STATES = Arrays.asList(values());
        CONSOLE_DONE_STATE = toArray(CarrierOrderExecuteState.DONE, CarrierOrderExecuteState.ABNORMAL_COMPLETED);
        CONSOLE_WAITING_STATE = toArray(
                CarrierOrderExecuteState.WAITING_NEXTSTOP,
                CarrierOrderExecuteState.WAITING_ROBOT,
                CarrierOrderExecuteState.INIT,
                CarrierOrderExecuteState.WAITING_RESOURCE,
                CarrierOrderExecuteState.PENDING_INIT_JOB,
                CarrierOrderExecuteState.WAITING_DISPATCHER,
                CarrierOrderExecuteState.WAITING_LIFT);
        CONSOLE_EXCEPTION_STATE = toArray(
                CarrierOrderExecuteState.SUSPEND,
                CarrierOrderExecuteState.PENDING,
                CarrierOrderExecuteState.CANCELED,
                CarrierOrderExecuteState.FAILED,
                CarrierOrderExecuteState.ABNORMAL_CANCEL);
        CONSOLE_EXECUTING_STATE = toArray(
                CarrierOrderExecuteState.INIT_JOB,
                CarrierOrderExecuteState.EXECUTING,
                CarrierOrderExecuteState.MOVE_START,
                CarrierOrderExecuteState.MOVE_DONE,
                CarrierOrderExecuteState.LIFT_UP_START,
                CarrierOrderExecuteState.LIFT_UP_DONE,
                CarrierOrderExecuteState.RACK_MOVE_START,
                CarrierOrderExecuteState.RACK_MOVE_DONE,
                CarrierOrderExecuteState.PUT_DOWN_START,
                CarrierOrderExecuteState.PUT_DOWN_DONE,
                CarrierOrderExecuteState.ENTER_STATION,
                CarrierOrderExecuteState.AGAIN_MOVE_START,
                CarrierOrderExecuteState.AGAIN_MOVE_DONE);
    }

    /**
     * 中控作业单列表显示待执行状态
     *
     * @return
     */
    public static List<String> getConsoleWaitingState() {
        return CONSOLE_WAITING_STATE;
    }

    /**
     * 中控作业单列表显示执行中状态
     *
     * @return
     */
    public static List<String> getConsoleExecutingState() {
        return CONSOLE_EXECUTING_STATE;
    }

    /**
     * 中控作业单列表显示异常状态
     *
     * @return
     */
    public static List<String> getConsoleExceptionState() {
        return CONSOLE_EXCEPTION_STATE;
    }

    /**
     * 中控作业单列表显示完成状态
     *
     * @return
     */
    public static List<String> getConsoleDoneState() {
        return CONSOLE_DONE_STATE;
    }

    public static List<CarrierOrderExecuteState> getMockStationEffectiveState() {
        return Arrays.asList(WAITING_NEXTSTOP, WAITING_ROBOT, INIT_JOB, MOVE_START, MOVE_DONE,
                LIFT_UP_START, LIFT_UP_DONE, RACK_MOVE_START, RACK_MOVE_DONE, PUT_DOWN_START, PUT_DOWN_DONE, ENTER_STATION);
    }

    /**
     * 获取任务组工作中的状态
     *
     * @return
     */
    public static List<CarrierOrderExecuteState> getJobGroupWorkingStates() {
        return Lists.newArrayList(
                WAITING_NEXTSTOP, WAITING_ROBOT, WAITING_LIFT, INIT_JOB, LIFT_UP_DONE, PUT_DOWN_DONE);
    }
}
