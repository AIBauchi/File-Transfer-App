from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.image import Image
from kivy.uix.textinput import TextInput
from kivy.animation import Animation
from kivy.uix.screenmanager import ScreenManager, Screen
import socket


class MainScreen(Screen):

    def __init__(self, **kwargs):
        super(MyLayout, self).__init__(**kwargs)
        self.orientation = 'vertical'
        self.name = socket.gethostname()

        # Navigation bar
        self.nav_bar = BoxLayout(size_hint=(1, 0.1))
        self.nav_bar.add_widget(Label(text='Encanto', size_hint=(0.7, 1), halign='center', font_name='Madrigal.ttf', font_size =29))
        navButton = Image(source='img/automatic.png', size_hint=(0.3, 1),)
        self.nav_bar.add_widget(navButton)
        self.nav_bar.bind(on_touch_down=self.on_nav_bar_touch_down)

        # Sidebar
        self.sidebar = BoxLayout(size_hint=(0.3, 1))
        self.sidebar_visible = False
        form_layout = BoxLayout(orientation='vertical', spacing=20)
        form_layout.add_widget(Label(text='Username', size_hint=(0.7, 0.1), halign='center', font_name='Buka Bird.ttf', color="orange", font_size =11))
        self.name_input = TextInput(hint_text='Name', size_hint_y= 0.1 )
        self.name_input.text = self.name
        self.name_input.multiline = False
        submit_button = Button(text='Save ↩', background_color='orange')
        submit_button.bind(on_press=self.on_submit_button_press)
        form_layout.add_widget(self.name_input)
        form_layout.add_widget(submit_button)
        self.sidebar.add_widget(form_layout)
        self.sidebar.opacity = 0
        self.add_widget(self.nav_bar)
        


        self.add_widget(BoxLayout(size_hint=(1, 0.1))) # Empty space
        self.add_widget(BoxLayout(size_hint=(1, 0.2))) # Empty space
        self.add_widget(self.sidebar)

        # Buttons 
        self.buttons_layout = BoxLayout(size_hint=(1, 0.4), padding=20)
        send_button = Button(text='Send', background_normal ='img/send.png',font_name='Buka Bird.ttf')
        receive_button = Button(text='Receive',  background_normal ='img/recieve.png', font_name='Buka Bird.ttf')
        receive_button.bind(on_press =self.click_recieve_button)
        self.buttons_layout.add_widget(send_button)
        self.buttons_layout.add_widget(receive_button)


        self.add_widget(BoxLayout(size_hint=(1, 0.1))) # Empty space
        self.add_widget(self.buttons_layout)


    def on_nav_bar_touch_down(self, instance, touch):
        if instance.collide_point(*touch.pos):
            # Toggle the visibility of the sidebar
            if self.sidebar_visible:
                self.sidebar_visible = False
                anim = Animation(opacity=0, duration=0.3)
                anim.start(self.sidebar)
            else:
                self.sidebar_visible = True
                anim = Animation(opacity=1, duration=0.3)
                anim.start(self.sidebar)

    def on_submit_button_press(self, instance):
        self.name = self.name_input.text
        # Do something with the name

        

        
        
        

class MyApp(App):
    def build(self):
        return MainScreen()


if __name__ == '__main__':
    MyApp().run()
