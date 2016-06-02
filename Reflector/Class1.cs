using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace DapperExtensions.Reflector
{
    class Class1
    {
        public static Action<object, object[]> CreatePropertiesAction(PropertyInfo[] infos)
        {
            Type classType = GetClassTypeByProperty(infos);
            DynamicMethod method = new DynamicMethod("", null, new Type[] { typeof(object), typeof(object[]) }, true);
            ILGenerator il = method.GetILGenerator();

            LocalBuilder obj = il.DeclareLocal(classType);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Unbox_Any, classType); //对要赋值的对象进行拆箱
            il.Emit(OpCodes.Stloc_0);

            for (int i = 0; i < infos.Length; i++)
            {
                Label lbl_end = il.DefineLabel();
                Type propType = infos[i].PropertyType;

                il.Emit(OpCodes.Ldarg_1);
                Ldc(il, i);
                il.Emit(OpCodes.Ldelem_Ref); //定位i处的value

                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Ceq);
                il.Emit(OpCodes.Brtrue_S, lbl_end); //判断是否为null，为null则跳过

                il.Emit(OpCodes.Ldloc_0); //对象压栈
                il.Emit(OpCodes.Ldarg_1); //值数组压栈
                Ldc(il, i);               //压入索引
                il.Emit(OpCodes.Ldelem_Ref); //取索引处的值
                il.Emit(OpCodes.Unbox_Any, propType); //拆箱

                il.Emit(OpCodes.Callvirt, infos[i].GetSetMethod()); //调用属性的set方法给属性赋值
                il.MarkLabel(lbl_end);
            }

            il.Emit(OpCodes.Ret);
            return method.CreateDelegate(typeof(Action<object, object[]>)) as Action<object, object[]>;
        }

        public static Func<object, object[]> CreatePropertiesFunc(PropertyInfo[] infos)
        {
            Type classType = GetClassTypeByProperty(infos);
            DynamicMethod method = new DynamicMethod("", typeof(object[]), new Type[] { typeof(object) }, true);
            ILGenerator il = method.GetILGenerator();

            LocalBuilder tmp = il.DeclareLocal(typeof(object));
            LocalBuilder result = il.DeclareLocal(typeof(object[]));

            LocalBuilder obj = il.DeclareLocal(classType);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Unbox_Any, classType);
            il.Emit(OpCodes.Stloc, obj);

            Ldc(il, infos.Length);
            il.Emit(OpCodes.Newarr, typeof(object));
            il.Emit(OpCodes.Stloc, result); //初始化一个object数组

            for (int i = 0; i < infos.Length; i++)
            {
                il.Emit(OpCodes.Ldloc, obj);
                il.Emit(OpCodes.Callvirt, infos[i].GetGetMethod()); //获取属性的值

                if (infos[i].PropertyType.IsValueType)
                    il.Emit(OpCodes.Box, infos[i].PropertyType); //值类型则装箱

                il.Emit(OpCodes.Stloc, tmp); //保存到临时变量

                il.Emit(OpCodes.Ldloc, result);
                Ldc(il, i);
                il.Emit(OpCodes.Ldloc, tmp); //数组对象、索引位置、值分别压栈
                il.Emit(OpCodes.Stelem_Ref); //赋值
            }

            il.Emit(OpCodes.Ldloc, result);
            il.Emit(OpCodes.Ret);

            return method.CreateDelegate(typeof(Func<object, object[]>)) as Func<object, object[]>;
        }

        private static Type GetClassTypeByProperty(PropertyInfo[] infos)
        {
            if (infos == null || infos.Length <= 0)
                throw new ArgumentNullException("infos");

            return infos[0].ReflectedType;
        }

        private static void Ldc(ILGenerator il, int value)
        {
            switch (value)
            {
                case -1:
                    il.Emit(OpCodes.Ldc_I4_M1);
                    return;
                case 0:
                    il.Emit(OpCodes.Ldc_I4_0);
                    return;
                case 1:
                    il.Emit(OpCodes.Ldc_I4_1);
                    return;
                case 2:
                    il.Emit(OpCodes.Ldc_I4_2);
                    return;
                case 3:
                    il.Emit(OpCodes.Ldc_I4_3);
                    return;
                case 4:
                    il.Emit(OpCodes.Ldc_I4_4);
                    return;
                case 5:
                    il.Emit(OpCodes.Ldc_I4_5);
                    return;
                case 6:
                    il.Emit(OpCodes.Ldc_I4_6);
                    return;
                case 7:
                    il.Emit(OpCodes.Ldc_I4_7);
                    return;
                case 8:
                    il.Emit(OpCodes.Ldc_I4_8);
                    return;
            }

            if (value > -129 && value < 128)
                il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
            else
                il.Emit(OpCodes.Ldc_I4, value);
        }
    }
}
