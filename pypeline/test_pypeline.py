from pypeline import Pipeline

def test_pipeline_run_without_dependencies():
    pipeline = Pipeline()

    @pipeline.task()
    def task1():
        return "task1"

    @pipeline.task(depends_on=task1)
    def task2(task1_result):
        return f"task2: {task1_result}"

    @pipeline.task(depends_on=task2)
    def task3(task2_result):
        return f"task3: {task2_result}"

    result_pipeline = pipeline.run()
    assert result_pipeline[task1] == "task1"
    assert result_pipeline[task2] == "task2: task1"
    assert result_pipeline[task3] == "task3: task2: task1"

def test_pipeline_run_with_parameter():
    pipeline_arg = Pipeline()

    @pipeline_arg.task()
    def task(args):
        return ' '.join(arg for arg in args)

    result_arg = pipeline_arg.run(["arg1", "arg2"])
    assert result_arg[task] == "arg1 arg2"

# TODO: Add unit tests