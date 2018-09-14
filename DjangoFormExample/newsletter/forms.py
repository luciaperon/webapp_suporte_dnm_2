#importing forms
from django import forms

#creating our forms
class SignupForm(forms.Form):
    #django gives a number of predefined fields
    #CharField and EmailField are only two of them
    #go through the official docs for more field details
    cpf = forms.CharField(label='Digite o CPF do aluno:', max_length=14)
    #periodo = forms.ChoiceField(label='Digita o Periodo:', max_length=5)
    #email = forms.EmailField(label='Enter your email', max_length=100)
    choices = (('20182', '20182',),('20181','20181',),('20172', '20172',))
    periodo = forms.ChoiceField(widget=forms.Select, choices=choices)
    #periodo.choices
    #[('1','20182'), ('2', '20181'), ('3', '20172')]
    #periodo.widget.choices
    #[('1', '20182'), ('2', '20181'), ('3', '20172')]
