# -*- coding: utf-8 -*-
"""
@author: Daniel.Salgado
"""
import pandas as pd
from colorama import init, Fore, Style

# Inicializa colorama
init(autoreset=True)

class nice:
    @staticmethod
    def print_header(title):
        print(f"\n{Fore.CYAN}{'*' * 40}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{title}")
        print(f"{Fore.CYAN}{'*' * 40}\n")

    @staticmethod
    def print_subheader(title):
        print(f"\n{Fore.GREEN}{'-' * 40}")
        print(f"{Fore.GREEN}{Style.BRIGHT}{title}")
        print(f"{Fore.GREEN}{'-' * 40}\n")

    @staticmethod
    def print_section(title):
        line = '*' * (len(title) + 8)
        print(f"\n{Fore.MAGENTA}{line}")
        print(f"{Fore.MAGENTA}*** {title} ***")
        print(f"{Fore.MAGENTA}{line}\n")

    @staticmethod
    def print_footer(title="Fin del Programa"):
        print(f"\n{Fore.BLUE}{'=' * 40}")
        print(f"{Fore.BLUE}{Style.BRIGHT}{title}")
        print(f"{Fore.BLUE}{'=' * 40}\n")

    @staticmethod
    def print_success(message):
        print(f"\n{Fore.GREEN}[SUCCESS] {message}{Style.RESET_ALL}\n")

    @staticmethod
    def print_error(message):
        print(f"\n{Fore.RED}[ERROR] {message}{Style.RESET_ALL}\n")

    @staticmethod
    def print_warning(message):
        print(f"\n{Fore.YELLOW}[WARNING] {message}{Style.RESET_ALL}\n")

    @staticmethod
    def print_info(message):
        print(f"\n{Fore.CYAN}[INFO] {message}{Style.RESET_ALL}\n")
    
    @staticmethod
    def print_debug(message):
        print(f"\n{Fore.WHITE}[DEBUG] {message}{Style.RESET_ALL}\n")
    
    @staticmethod
    def print_critical(message):
        print(f"\n{Fore.RED}{Style.BRIGHT}[CRITICAL] {message}{Style.RESET_ALL}\n")
    
    @staticmethod
    def print_table(df: pd.DataFrame, title="Datos"):
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{title}:\n{Style.RESET_ALL}")
        print(df.to_string(index=False))
        print("\n")
    
    @staticmethod
    def print_list(items, title="Lista"):
        print(f"\n{Fore.GREEN}{Style.BRIGHT}{title}:\n{Style.RESET_ALL}")
        for idx, item in enumerate(items, 1):
            print(f"  {Fore.WHITE}{idx}. {item}")
        print("\n")
    
    @staticmethod
    def print_steps(steps, title="Pasos a Seguir"):
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}{title}:\n{Style.RESET_ALL}")
        for idx, step in enumerate(steps, 1):
            print(f"  {Fore.WHITE}Paso {idx}: {step}")
        print("\n")
    
    @staticmethod
    def print_divider(char='-', length=50):
        print(f"\n{Fore.WHITE}{char * length}\n")
    
    @staticmethod
    def print_json(data, title="JSON Data"):
        import json
        formatted_json = json.dumps(data, indent=4, ensure_ascii=False)
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{title}:\n{Style.RESET_ALL}")
        print(formatted_json)
        print("\n")
    
    @staticmethod
    def print_quote(quote, author="Autor"):
        print(f"\n{Fore.YELLOW}“{quote}”\n  - {author}\n{Style.RESET_ALL}")
    
    @staticmethod
    def print_warning_box(message):
        border = '*' * (len(message) + 4)
        print(f"\n{Fore.YELLOW}{border}")
        print(f"{Fore.YELLOW}* {message} *")
        print(f"{Fore.YELLOW}{border}{Style.RESET_ALL}\n")
    
    @staticmethod
    def print_error_box(message):
        border = '*' * (len(message) + 4)
        print(f"\n{Fore.RED}{border}")
        print(f"{Fore.RED}* {message} *")
        print(f"{Fore.RED}{border}{Style.RESET_ALL}\n")
    
    @staticmethod
    def print_bold(message):
        print(f"{Style.BRIGHT}{Fore.WHITE}{message}{Style.RESET_ALL}")
    
    @staticmethod
    def print_italic(message):
        # Nota: La consola estándar no soporta texto en cursiva; se simula con DIM.
        print(f"{Style.DIM}{Fore.WHITE}{message}{Style.RESET_ALL}")

