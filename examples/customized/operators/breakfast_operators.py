from airflow.models import BaseOperator


class MakeBreadOperator(BaseOperator):
    template_fields = ("bread_type",)

    def __init__(self, bread_type, *args, **kwargs):
        super(MakeBreadOperator, self).__init__(*args, **kwargs)
        self.bread_type = bread_type

    def execute(self, context):
        print("Make {} bread".format(self.bread_type))


class MakeCoffeeOperator(BaseOperator):
    template_fields = ("coffee_type",)

    def __init__(self, coffee_type, *args, **kwargs):
        super(MakeCoffeeOperator, self).__init__(*args, **kwargs)
        self.coffee_type = coffee_type

    def execute(self, context):
        print("Make {} bread".format(self.coffee_type))
